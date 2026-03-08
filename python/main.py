import fastf1
import pandas as pd
import duckdb
import os
import datetime

# ----------------------
# Paths and cache
# ----------------------
db_dir = os.path.join("..", "database")
os.makedirs(db_dir, exist_ok=True)
db_path = os.path.join(db_dir, "f1_data.duckdb")

cache_dir = "cache"
os.makedirs(cache_dir, exist_ok=True)
fastf1.Cache.enable_cache(cache_dir)

# ----------------------
# Season settings
# ----------------------
SEASONS = [2025, 2026]
SESSION_TYPE = "R"

today = datetime.datetime.utcnow()

# ----------------------
# Connect to DuckDB
# ----------------------
con = duckdb.connect(db_path)
print("Connecting to DuckDB...")
print("DB PATH:", os.path.abspath(db_path))

# ----------------------
# Create tables
# ----------------------
con.execute("""
CREATE TABLE IF NOT EXISTS drivers(
driverId VARCHAR,
driver VARCHAR,
team VARCHAR
)
""")

con.execute("""
CREATE TABLE IF NOT EXISTS races(
raceId VARCHAR,
season INTEGER,
round INTEGER,
circuit VARCHAR
)
""")

con.execute("""
CREATE TABLE IF NOT EXISTS laps(
raceId VARCHAR,
driverId VARCHAR,
lap INTEGER,
lap_time DOUBLE,
compound VARCHAR,
stint INTEGER,
position INTEGER
)
""")

con.execute("""
CREATE TABLE IF NOT EXISTS results(
raceId VARCHAR,
driverId VARCHAR,
position INTEGER,
points DOUBLE
)
""")

con.execute("""
CREATE TABLE IF NOT EXISTS pit_stops(
raceId VARCHAR,
driverId VARCHAR,
lap INTEGER,
duration DOUBLE
)
""")

con.execute("""
CREATE TABLE IF NOT EXISTS telemetry(
raceId VARCHAR,
driverId VARCHAR,
distance DOUBLE,
x DOUBLE,
y DOUBLE,
speed DOUBLE,
throttle DOUBLE,
brake DOUBLE,
rpm DOUBLE,
gear INTEGER,
drs INTEGER
)
""")

# ----------------------
# MAIN LOOP (SEASONS)
# ----------------------
for SEASON in SEASONS:

    print(f"\n===== LOADING SEASON {SEASON} =====")

    schedule = fastf1.get_event_schedule(SEASON)

    for _, race in schedule.iterrows():

        race_name = race["EventName"]
        round_number = race["RoundNumber"]
        race_date = race["EventDate"]

        # skip preseason testing
        if round_number == 0:
            continue

        # skip NaT dates
        if pd.isna(race_date):
            continue
         
        race_id = f"{SEASON}_{round_number}"
        
        # skip future races
        if race_date > today:
            print(f"Skipping future race: {race_name}")
            continue

        existing = con.execute(
            "SELECT COUNT(*) FROM races WHERE raceId = ?", [race_id]
        ).fetchone()[0]

        if existing > 0:
            print(f"Race {race_id} already loaded, skipping.")
            continue

        print(f"Loading round {round_number}: {race_name}")

        try:
            session = fastf1.get_session(SEASON, round_number, SESSION_TYPE)
            session.load()
        except Exception as e:
            print(f"Session skipped: {e}")
            continue

        laps = session.laps
        results = session.results
        drivers = session.drivers

        if laps is None or results is None:
            print("Session has no data yet.")
            continue

        if laps.empty or results.empty:
            print("Empty datasets, skipping.")
            continue

        # ----------------------
        # DRIVERS
        # ----------------------
        drivers_df = pd.DataFrame({"driverId": drivers})
        drivers_df["driver"] = drivers_df["driverId"]

        team_map = laps[["Driver", "Team"]].drop_duplicates()
        team_map.columns = ["driverId", "team"]

        drivers_df = drivers_df.merge(team_map, on="driverId", how="left")

        con.register("drivers_df", drivers_df)

        con.execute("""
        INSERT INTO drivers
        SELECT * FROM drivers_df
        WHERE driverId NOT IN (SELECT driverId FROM drivers)
        """)

        # ----------------------
        # RACES
        # ----------------------
        race_df = pd.DataFrame({
            "raceId":[race_id],
            "season":[SEASON],
            "round":[round_number],
            "circuit":[race_name]
        })

        con.register("race_df", race_df)
        con.execute("INSERT INTO races SELECT * FROM race_df")

        # ----------------------
        # LAPS
        # ----------------------
        laps_df = laps[["Driver","LapNumber","LapTime","Compound","Stint","Position"]].copy()

        laps_df.columns = [
            "driverId","lap","lap_time","compound","stint","position"
        ]

        laps_df["raceId"] = race_id
        laps_df["lap_time"] = laps_df["lap_time"].dt.total_seconds()

        laps_df = laps_df[[
            "raceId","driverId","lap","lap_time","compound","stint","position"
        ]]

        con.register("laps_df", laps_df)
        con.execute("INSERT INTO laps SELECT * FROM laps_df")

        # ----------------------
        # RESULTS
        # ----------------------
        results_df = results[["Abbreviation","Position","Points"]].copy()

        results_df.columns = ["driverId","position","points"]
        results_df["raceId"] = race_id

        results_df = results_df[[
            "raceId","driverId","position","points"
        ]]

        con.register("results_df", results_df)
        con.execute("INSERT INTO results SELECT * FROM results_df")

        # ----------------------
        # PIT STOPS
        # ----------------------
        pit_df = laps[laps["PitOutTime"].notna()][["Driver","LapNumber"]].copy()

        pit_df.columns = ["driverId","lap"]
        pit_df["raceId"] = race_id
        pit_df["duration"] = None

        pit_df = pit_df[[
            "raceId","driverId","lap","duration"
        ]]

        con.register("pit_df", pit_df)
        con.execute("INSERT INTO pit_stops SELECT * FROM pit_df")

        # ----------------------
        # TELEMETRY
        # ----------------------
        telemetry_rows = []

        for driver in drivers:

            try:
                driver_laps = laps.pick_drivers(driver)
                fastest = driver_laps.pick_fastest()

                car_data = fastest.get_car_data().add_distance()
                pos_data = fastest.get_pos_data()

                cols = ["Speed","Throttle","Brake","RPM","nGear","DRS","Distance"]

                for col in cols:
                    if col not in car_data.columns:
                        car_data[col] = pd.NA

                df = car_data[cols].copy()

                df.columns = [
                    "speed","throttle","brake","rpm","gear","drs","distance"
                ]

                pos_df = pos_data[["X","Y"]].copy()
                pos_df.columns = ["x","y"]

                df = df.reset_index(drop=True)
                pos_df = pos_df.reset_index(drop=True)

                df = pd.concat([df,pos_df], axis=1)

                df["driverId"] = driver
                df["raceId"] = race_id

                telemetry_rows.append(df)

            except Exception as e:
                print(f"Telemetry skipped for {driver}: {e}")

        if telemetry_rows:

            telemetry_df = pd.concat(telemetry_rows)

            telemetry_df = telemetry_df[[
                "raceId","driverId","distance","x","y",
                "speed","throttle","brake","rpm","gear","drs"
            ]]

            con.register("telemetry_df", telemetry_df)
            con.execute("INSERT INTO telemetry SELECT * FROM telemetry_df")

# ----------------------
# DATA CHECK
# ----------------------
print("\n===== DATABASE CHECK =====")

tables = ["drivers","races","laps","results","pit_stops","telemetry"]

for table in tables:
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"{table}: {count} rows")

con.close()

print("\nUpdate finished.")
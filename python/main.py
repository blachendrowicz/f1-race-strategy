import fastf1
import pandas as pd
import duckdb
import os
import datetime
import time

# ----------------------
# Paths
# ----------------------

db_dir = os.path.join("..", "database")
os.makedirs(db_dir, exist_ok=True)
db_path = os.path.join(db_dir, "f1_data.duckdb")

cache_dir = "cache"
os.makedirs(cache_dir, exist_ok=True)
fastf1.Cache.enable_cache(cache_dir)

SEASONS = [2025, 2026]
today = datetime.datetime.now(datetime.UTC)

# ----------------------
# Connect DB
# ----------------------

con = duckdb.connect(db_path)

print("Connecting to DuckDB...")
print("DB PATH:", os.path.abspath(db_path))

# ----------------------
# TABLES
# ----------------------

con.execute("""
CREATE TABLE IF NOT EXISTS drivers(
driverId VARCHAR,
driver VARCHAR
)
""")

con.execute("""
CREATE TABLE IF NOT EXISTS driver_races(
raceId VARCHAR,
driverId VARCHAR,
driver_number INTEGER,
team VARCHAR
)
""")

con.execute("""
CREATE TABLE IF NOT EXISTS races(
raceId VARCHAR,
season INTEGER,
round INTEGER,
gp_name VARCHAR,
location VARCHAR,
country VARCHAR
)
""")

con.execute("""
CREATE TABLE IF NOT EXISTS sectors(
raceId VARCHAR,
sector INTEGER,
start_distance DOUBLE
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
time DOUBLE,
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

con.execute("""
CREATE TABLE IF NOT EXISTS qualifying(
raceId VARCHAR,
driverId VARCHAR,
q1 DOUBLE,
q2 DOUBLE,
q3 DOUBLE,
position INTEGER
)
""")

# ----------------------
# MAIN LOOP
# ----------------------

for SEASON in SEASONS:

    print(f"\n===== SEASON {SEASON} =====")

    try:
        schedule = fastf1.get_event_schedule(SEASON)
    except Exception as e:
        print("Schedule error:", e)
        continue

    for _, race in schedule.iterrows():

        round_number = race["RoundNumber"]

        if round_number == 0:
            continue

        race_date = race["EventDate"]

        if pd.isna(race_date):
            continue

        if race_date.tz_localize("UTC") > today:
            print("Skipping future race:", race["EventName"])
            continue

        race_id = f"{SEASON}_{round_number}"

        exists = con.execute(
            "SELECT COUNT(*) FROM races WHERE raceId = ?",
            [race_id]
        ).fetchone()[0]

        if exists:
            print("Already loaded:", race_id)
            continue

        print("Loading:", race["EventName"])

        # ----------------------
        # RACE SESSION
        # ----------------------

        try:
            session = fastf1.get_session(SEASON, round_number, "R")
            session.load()
        except Exception as e:
            print("Session skipped:", e)
            continue

        laps = session.laps.copy()
        results = session.results.copy()

        if laps.empty:
            continue

        # ----------------------
        # DRIVERS
        # ----------------------

        drivers_df = results[["Abbreviation","FullName"]].drop_duplicates()
        drivers_df.columns = ["driverId","driver"]

        con.register("drivers_df", drivers_df)

        con.execute("""
        INSERT INTO drivers
        SELECT *
        FROM drivers_df
        WHERE driverId NOT IN (SELECT driverId FROM drivers)
        """)

        # ----------------------
        # DRIVER RACES
        # ----------------------

        dr = laps[["Driver","DriverNumber","Team"]].drop_duplicates()

        dr.columns = ["driverId","driver_number","team"]
        dr["raceId"] = race_id

        dr = dr[[
        "raceId",
        "driverId",
        "driver_number",
        "team"
        ]]

        con.register("dr", dr)
        con.execute("INSERT INTO driver_races SELECT * FROM dr")

        # ----------------------
        # RACES
        # ----------------------

        race_df = pd.DataFrame({

        "raceId":[race_id],
        "season":[SEASON],
        "round":[round_number],
        "gp_name":[race["EventName"]],
        "location":[race["Location"]],
        "country":[race["Country"]]

        })

        con.register("race_df", race_df)
        con.execute("INSERT INTO races SELECT * FROM race_df")

        # ----------------------
        # SECTORS
        # ----------------------

        try:

            circuit_info = session.get_circuit_info()

            sector_df = pd.DataFrame({

                "raceId":[race_id,race_id,race_id],
                "sector":[1,2,3],
                "start_distance":[
                    0,
                    circuit_info.sector_1,
                    circuit_info.sector_2
                ]

            })

            con.register("sector_df", sector_df)
            con.execute("INSERT INTO sectors SELECT * FROM sector_df")

        except Exception as e:

            print("Sector data skipped:", e)

        # ----------------------
        # LAPS
        # ----------------------

        laps_df = laps[[
            "Driver",
            "LapNumber",
            "LapTime",
            "Compound",
            "Stint",
            "Position"
        ]].copy()

        laps_df.columns = [
            "driverId",
            "lap",
            "lap_time",
            "compound",
            "stint",
            "position"
        ]

        laps_df["raceId"] = race_id
        laps_df["lap_time"] = laps_df["lap_time"].dt.total_seconds()

        laps_df = laps_df[[
            "raceId",
            "driverId",
            "lap",
            "lap_time",
            "compound",
            "stint",
            "position"
        ]]

        con.register("laps_df", laps_df)
        con.execute("INSERT INTO laps SELECT * FROM laps_df")

        # ----------------------
        # RESULTS
        # ----------------------

        results_df = results[[
            "Abbreviation",
            "Position",
            "Points"
        ]]

        results_df.columns = [
            "driverId",
            "position",
            "points"
        ]

        results_df["raceId"] = race_id

        results_df = results_df[[
            "raceId",
            "driverId",
            "position",
            "points"
        ]]

        con.register("results_df", results_df)
        con.execute("INSERT INTO results SELECT * FROM results_df")

        # ----------------------
        # TELEMETRY
        # ----------------------

        telemetry_rows = []

        for driver in session.drivers:

            try:

                fastest = laps.pick_drivers(driver).pick_fastest()

                car = fastest.get_car_data().add_distance().reset_index(drop=True)
                pos = fastest.get_pos_data().reset_index(drop=True)

                df = car[[
                    "Time",
                    "Speed",
                    "Throttle",
                    "Brake",
                    "RPM",
                    "nGear",
                    "DRS",
                    "Distance"
                ]].copy()

                df.columns = [
                    "time",
                    "speed",
                    "throttle",
                    "brake",
                    "rpm",
                    "gear",
                    "drs",
                    "distance"
                ]

                df["time"] = df["time"].dt.total_seconds()

                pos_df = pos[["X","Y"]]
                pos_df.columns = ["x","y"]

                df = pd.concat([df,pos_df],axis=1)

                df["driverId"] = driver
                df["raceId"] = race_id

                telemetry_rows.append(df)

            except Exception as e:

                print("Telemetry skipped:", driver)

        if telemetry_rows:

            telemetry_df = pd.concat(telemetry_rows)

            telemetry_df = telemetry_df[[

                "raceId",
                "driverId",
                "time",
                "distance",
                "x",
                "y",
                "speed",
                "throttle",
                "brake",
                "rpm",
                "gear",
                "drs"

            ]]

            con.register("telemetry_df", telemetry_df)
            con.execute("INSERT INTO telemetry SELECT * FROM telemetry_df")

        # ----------------------
        # QUALIFYING
        # ----------------------

        try:

            quali = fastf1.get_session(SEASON, round_number, "Q")
            quali.load()

            qres = quali.results[[
                "Abbreviation",
                "Q1",
                "Q2",
                "Q3",
                "Position"
            ]].copy()

            qres.columns = [
                "driverId",
                "q1",
                "q2",
                "q3",
                "position"
            ]

            qres["raceId"] = race_id

            for col in ["q1","q2","q3"]:
                qres[col] = qres[col].dt.total_seconds()

            qres = qres[[

                "raceId",
                "driverId",
                "q1",
                "q2",
                "q3",
                "position"

            ]]

            con.register("qres", qres)
            con.execute("INSERT INTO qualifying SELECT * FROM qres")

        except Exception as e:

            print("Qualifying skipped")

        time.sleep(2)

# ----------------------
# CHECK
# ----------------------

print("\n===== DATABASE CHECK =====")

tables = [
"drivers",
"driver_races",
"races",
"sectors",
"laps",
"results",
"pit_stops",
"telemetry",
"qualifying"
]

for t in tables:

    count = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(t, ":", count)

con.close()

print("\nUpdate finished.")
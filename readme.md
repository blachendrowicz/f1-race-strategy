# Race Strategy Analytics – Formula 1

## Overview

This project analyzes Formula 1 race performance and strategy using telemetry and lap timing data.
The goal is to build a full analytics pipeline from raw race data to interactive dashboards.

The project focuses on:

* race pace analysis
* tire strategy and pit stops
* driver performance comparison
* telemetry analysis and track speed visualization

> **Note:** Currently, the project contains only the Python ETL code and DuckDB database. 
> SQL analyses and Power BI dashboards will be implemented in the future.

## Data Pipeline

FastF1 API  
→ Python ETL  
→ DuckDB database  
→ SQL analysis *(future)*  
→ Power BI dashboard *(future)*

## Data Source

Data is collected using the FastF1 Python library, which provides access to official Formula 1 live timing data.

## Database Schema

### drivers

| column   | description         |
| -------- | ------------------- |
| driverId | driver abbreviation |
| driver   | driver name         |
| team     | constructor team    |

### races

| column  | description     |
| ------- | --------------- |
| raceId  | race identifier |
| season  | season year     |
| round   | race round      |
| circuit | circuit name    |

### laps

| column   | description        |
| -------- | ------------------ |
| raceId   | race identifier    |
| driverId | driver identifier  |
| lap      | lap number         |
| lap_time | lap time (seconds) |
| compound | tire compound      |
| stint    | stint number       |
| position | track position     |

### results

| column   | description         |
| -------- | ------------------- |
| raceId   | race identifier     |
| driverId | driver identifier   |
| position | finishing position  |
| points   | championship points |

### pit_stops

| column   | description       |
| -------- | ----------------- |
| raceId   | race identifier   |
| driverId | driver identifier |
| lap      | lap number        |
| duration | pit stop duration |

### telemetry

| column   | description          |
| -------- | -------------------- |
| raceId   | race identifier      |
| driverId | driver identifier    |
| distance | distance along track |
| x        | X position on track  |
| y        | Y position on track  |
| speed    | car speed            |
| throttle | throttle input       |
| brake    | brake input          |
| rpm      | engine RPM           |
| gear     | current gear         |
| drs      | DRS status           |

## Key Analyses

SQL queries include *(planned for future implementation)*:

* average race pace per driver
* fastest laps
* tire degradation by stint
* pit stop strategy
* telemetry speed analysis

## Power BI Dashboard *(planned)*

The dashboard will contain three main pages:

### Race Overview

* position vs lap
* pit stop strategy
* fastest laps

### Driver Performance

* lap time distribution
* tire degradation
* average race pace

### Telemetry Analysis

* track map with speed visualization
* driver speed comparison
* throttle and brake usage

The telemetry page visualizes driver speed directly on the track using X/Y coordinates from telemetry data.

## Technologies

* Python
* FastF1
* DuckDB
* SQL *(future)*
* Power BI *(future)*
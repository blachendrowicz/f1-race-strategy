-- Tworzymy widok dla Power BI: mapa toru z prędkością
CREATE OR REPLACE VIEW telemetry_map_view AS
SELECT
    t.raceId,
    t.driverId,
    t.distance,
    t.x,
    t.y,
    t.speed,
    t.throttle,
    t.brake,
    t.rpm,
    t.gear,
    t.drs
FROM telemetry t
JOIN races r ON t.raceId = r.raceId
WHERE r.season = 2026
  AND r.raceId IN (
      SELECT raceId
      FROM races
      WHERE DATE(raceId) <= CURRENT_DATE  -- tylko zakończone wyścigi
  )
ORDER BY t.raceId, t.driverId, t.distance;
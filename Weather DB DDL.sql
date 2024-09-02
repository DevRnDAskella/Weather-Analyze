DROP TABLE IF EXISTS metar_lake;
DROP TABLE IF EXISTS metar;
DROP TABLE IF EXISTS taf_lake;
CREATE TABLE IF NOT EXISTS metar_lake (
  id SERIAL PRIMARY KEY,
  datetime TIMESTAMP,
  airport VARCHAR(4),
  metar TEXT
);
CREATE TABLE IF NOT EXISTS taf_lake (
  id SERIAL PRIMARY KEY,
  datetime TIMESTAMP,
  airport VARCHAR(4),
  taf TEXT
);
CREATE TABLE IF NOT EXISTS metar (
  id SERIAL PRIMARY KEY,
  datetime TIMESTAMP,
  airport VARCHAR(4),
  wind_direct VARCHAR(5),
  wind_power INTEGER,
  wind_gust INTEGER,
  wind_uom VARCHAR(5),
  wind_variable VARCHAR(10),
  temp INTEGER,
  dew_point INTEGER,
  metar TEXT
);
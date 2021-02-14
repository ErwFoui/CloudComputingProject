CREATE KEYSPACE IF NOT EXISTS task2 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

DROP TABLE IF EXISTS task2.flights;
CREATE TABLE task2.flights(
    YEAR int,
    MONTH int,
    DAY_OF_MONTH int,
    DAY_OF_WEEK int,
    FL_DATE text,
    OP_UNIQUE_CARRIER text,
    OP_CARRIER_FL_NUM int,
    ORIGIN_AIRPORT_ID int,
    ORIGIN text,
    DEST_AIRPORT_ID int,
    DEST text,
    CRS_DEP_TIME int,
    DEP_TIME int,
    DEP_DELAY int,
    DEP_DELAY_NEW int,
    CRS_ARR_TIME int,
    ARR_TIME int,
    ARR_DELAY int,
    ARR_DELAY_NEW int,
    CANCELLED int,
    ID bigint,
PRIMARY KEY (ID)
);

DROP TABLE IF EXISTS task2.flights_2_1;
CREATE TABLE task2.flights_2_1 (
  origin text,
  carrier text,
  avg_delay text,
  rank bigint,
PRIMARY KEY (origin, rank)
);

DROP TABLE IF EXISTS task2.flights_2_2;
CREATE TABLE task2.flights_2_2 (
  origin text,
  destination text,
  avg_delay double,
  rank bigint,
PRIMARY KEY (origin, rank)
);

DROP TABLE IF EXISTS task2.flights_2_3;
CREATE TABLE task2.flights_2_3 (
  origin text,
  destination text,
  carrier text,
  avg_delay double,
  rank bigint,
PRIMARY KEY (origin, destination, rank)
);

DROP TABLE IF EXISTS task2.flights_2_4;
CREATE TABLE task2.flights_2_4 (
  origin text,
  destination text,
  avg_delay double,
PRIMARY KEY (origin, destination)
);

DROP TABLE IF EXISTS task2.flights_3_2;
CREATE TABLE task2.flights_3_2 (
  x text,
  y text,
  z text,
  xy_fl_date text,
  xy_dep_time text,
  carrier_xy text,
  flight_xy text,
  yz_fl_date text,
  yz_dep_time text,
  carrier_yz text,
  flight_yz text,
  total_delay double,
PRIMARY KEY (x, y, z, xy_FL_DATE, yz_FL_DATE)
);

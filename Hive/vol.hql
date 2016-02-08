-- Import raw data
CREATE TABLE doc (
  datestamp string,
  open double,
  high double,
  low double,
  close double,
  volume double,
  adjclose double
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/gpfs/courses/cse687/spring2015/data/hw1/small'  INTO TABLE doc;

-- Add stock name (file name)
CREATE TABLE doc2 (
  stock string,
  datestamp string,
  open double,
  high double,
  low double,
  close double,
  volume double,
  adjclose double
);

INSERT INTO TABLE doc2 SELECT regexp_extract(INPUT__FILE__NAME,'([A-Z]+[A-Za-z0-1-]*)(\.csv)',0),* FROM doc WHERE datestamp != 'Date';

-- Discard unwanted fields, split filename to get stock name, divide datestamp to year, month, day
CREATE TABLE doc3 (
  stock string,
  date_year int,
  date_month int,
  date_day int,
  adjclose double
);

INSERT INTO TABLE doc3 SELECT SPLIT( stock, '.csv')[0], SUBSTRING(datestamp, 0,4), SUBSTRING(datestamp, 6,2), SUBSTRING(datestamp, 9,2), adjclose FROM doc2;

-- Get beginning and ending values in each month
CREATE TABLE grp_minmax (
  stock string,
  date_year int,
  date_month int,
  min_day int,
  max_day int
);

INSERT INTO TABLE grp_minmax SELECT stock, date_year, date_month, MIN(date_day), MAX(date_day) FROM doc3
GROUP BY stock, date_year, date_month;

-- Join to get stock values on min and max days
CREATE TABLE doc4 (
  stock string,
  date_year int,
  date_month int,
  date_day int,
  adjclose double);

INSERT INTO TABLE doc4 SELECT doc3.stock, doc3.date_year, doc3.date_month, doc3.date_day, doc3.adjclose
FROM grp_minmax LEFT JOIN doc3
WHERE doc3.stock = grp_minmax.stock
AND doc3.date_year = grp_minmax.date_year
AND doc3.date_month = grp_minmax.date_month
AND (doc3.date_day = grp_minmax.min_day OR doc3.date_day = grp_minmax.max_day);

-- Self join to bring min and max month values in the same row
CREATE TABLE doc5 (
  stock string,
  date_year int,
  date_month int,
  min_day double,
  max_day double
);

INSERT INTO TABLE doc5 SELECT DISTINCT one.stock, one.date_year, one.date_month, one.adjclose, two.adjclose FROM grp_minmax, doc4 one JOIN doc4 two
ON one.stock = two.stock
AND one.date_year = two.date_year
AND one.date_month = two.date_month
WHERE one.date_day = grp_minmax.min_day AND two.date_day = grp_minmax.max_day;

-- Calculate xi for each month
CREATE TABLE doc6 (
  stock string,
  x double
);

INSERT INTO TABLE doc6 SELECT stock, IF(min_day == 0, 0, (max_day - min_day) / min_day) FROM doc5;

-- Calculate overall average
CREATE TABLE doc7 (
  stock string,
  avg double
);

INSERT INTO TABLE doc7 SELECT stock, AVG(x) FROM doc6 GROUP BY stock;

-- Calculate counter (N-1)
CREATE TABLE t1 (
  stock string,
  counter int
);

INSERT INTO TABLE t1 SELECT stock, COUNT(min_day) - 1 as counter FROM doc5
GROUP BY stock;

-- Calculate squared distance
CREATE TABLE t2 (
  stock string,
  sq double
);

INSERT INTO TABLE t2 SELECT doc6.stock, (doc6.x - doc7.avg) * (doc6.x - doc7.avg) as sq
FROM doc6, doc7
WHERE doc6.stock = doc7.stock;

-- Combine squared distance and count
CREATE TABLE doc8 (
  stock string,
  counter int,
  sq double
);

INSERT INTO TABLE doc8 SELECT one.stock, one.counter, two.sq FROM t1 as one, t2 as two
WHERE one.stock = two.stock;

-- Calculate volatility
CREATE TABLE doc9 (
  stock string,
  vol double
);

INSERT INTO TABLE doc9 SELECT stock, SQRT(SUM(sq) / counter) as vol FROM doc8
GROUP BY stock, counter
HAVING counter > 1
ORDER BY vol;

-- Sort and select top 10
SELECT stock, vol FROM doc9
ORDER BY vol ASC LIMIT 10;

SELECT stock, vol FROM doc9
ORDER BY vol DESC LIMIT 10;
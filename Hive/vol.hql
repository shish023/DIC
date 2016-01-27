CREATE TABLE doc (
  date string,
  open double,
  high double,
  low double,
  close double,
  volume double,
  adjclose double
)

row format delimited
fields terminated by ','
lines terminated by '\n' 
stored as textfile;

LOAD DATA LOCAL INPATH '/gpfs/courses/cse687/spring2015/data/hw1/small'  INTO TABLE doc;

CREATE TABLE doc2 (
  stock string,
  date string,
  open double,
  high double,
  low double,
  close double,
  volume double,
  adjclose double
);

INSERT INTO TABLE doc2 SELECT regexp_extract(INPUT__FILE__NAME,'([A-Z]+[A-Za-z0-1-]*)(\.csv)',0),* FROM doc WHERE date != 'Date';

CREATE TABLE doc3 (
  stock string,
  year int,
  month int,
  day int,
  adjclose double
);

INSERT INTO TABLE doc3 SELECT SPLIT( stock, '.csv')[0], SUBSTRING(date, 0,4), SUBSTRING(date, 6,2), SUBSTRING(date, 9,2), adjclose FROM doc2;


CREATE TABLE grp_minmax (
  stock string,
  year int,
  month int,
  min int,
  max int
);

INSERT INTO TABLE grp_minmax SELECT stock, year, month, MIN(day), MAX(day) FROM doc3
GROUP BY stock,year,month;

CREATE TABLE doc4 (
  stock string,
  year int,
  month int,
  day int,
  adjclose double);

INSERT INTO TABLE doc4 SELECT doc3.stock, doc3.year, doc3.month, doc3.day, doc3.adjclose FROM grp_minmax LEFT JOIN doc3
WHERE doc3.stock = grp_minmax.stock
AND doc3.year = grp_minmax.year
AND doc3.month = grp_minmax.month
AND (doc3.day = grp_minmax.min OR doc3.day = grp_minmax.max);


CREATE TABLE doc5 (
  stock string,
  year int,
  month int,
  min double,
  max double
);

INSERT INTO TABLE doc5 SELECT DISTINCT one.stock, one.year, one.month, one.adjclose, two.adjclose FROM grp_minmax, doc4 one JOIN doc4 two
ON one.stock = two.stock
AND one.year = two.year
AND one.month = two.month
WHERE one.day = grp_minmax.min AND two.day = grp_minmax.max;

CREATE TABLE doc6 (
  stock string,
  x double
);

INSERT INTO TABLE doc6 SELECT stock, IF(min == 0, 0, (max - min) / min) FROM doc5;

CREATE TABLE doc7 (
  stock string,
  avg double
);

INSERT INTO TABLE doc7 SELECT stock, AVG(x) FROM doc6 GROUP BY stock;

CREATE TABLE t1 (
  stock string,
  count int
);

INSERT INTO TABLE t1 SELECT stock, COUNT(min) - 1 as count FROM doc5
GROUP BY stock;

CREATE TABLE t2 (
  stock string,
  sq double
);

INSERT INTO TABLE t2 SELECT doc6.stock, (doc6.x - doc7.avg) * (doc6.x - doc7.avg) as sq 
FROM doc6, doc7
WHERE doc6.stock = doc7.stock;

CREATE TABLE doc8 (
  stock string,
  count int,
  sq double
);

INSERT INTO TABLE doc8 SELECT one.stock, one.count, two.sq FROM t1 as one, t2 as two
WHERE one.stock = two.stock;

CREATE TABLE doc9 (
  stock string,
  vol double
); 

INSERT INTO TABLE doc9 SELECT stock, SQRT(SUM(sq) / count) as vol FROM doc8
GROUP BY stock, count
HAVING count > 1
ORDER BY vol;

SELECT stock, vol FROM doc9
ORDER BY vol ASC LIMIT 10;

SELECT stock, vol FROM doc9
ORDER BY vol DESC LIMIT 10;



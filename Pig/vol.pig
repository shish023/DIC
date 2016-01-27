raw = LOAD 'hdfs:///pigdata' USING PigStorage(',','-tagFile');
data = FILTER raw by $1 != 'Date';

imp = FOREACH data GENERATE (chararray)$0, (double)$7, (double)SUBSTRING($1, 0, 4), (double)SUBSTRING($1, 5, 7), (double)SUBSTRING($1, 8, 10);

grp = GROUP imp by ($0, $2, $3);

monthlytemp = FOREACH grp {
ascdata = ORDER imp BY $4 ASC;
low = LIMIT ascdata 1;
descdata = ORDER imp BY $4 DESC;
high = LIMIT descdata 1;
GENERATE $0.$0 as stock, flatten(high.$1) as a:double, flatten(low.$1) as b:double;
}

month = FOREACH monthlytemp GENERATE stock as s, (b == 0? 0:(a-b)/b) as xi:double;

avgtemp = GROUP month BY s;

avg = FOREACH avgtemp GENERATE $0 as stock, AVG(month.xi) as a;

vol_join = JOIN month by s, avg by stock;

vol_sq = FOREACH vol_join GENERATE stock, (xi - a) * (xi - a) as sigma;

vol_temp = GROUP vol_sq BY stock;

vol_sum = FOREACH vol_temp GENERATE $0 as stock, SUM(vol_sq.sigma) as sum, (double)COUNT(vol_sq.sigma) as n:double;

vol_temp2 = FOREACH vol_sum GENERATE stock, sum, n as count, 1 / ((double)n - 1) as n:double;

vol = FOREACH vol_temp2 GENERATE stock, (count == 1? 0:SQRT(n*sum)) as v;

vol_order_asc = ORDER vol BY v ASC;
vol_order_desc = ORDER vol BY v DESC;

least = LIMIT vol_order_asc 10;
most = LIMIT vol_order_desc 10;

out = UNION least, most;

STORE out INTO 'hdfs:///pigdata/hw3_out';
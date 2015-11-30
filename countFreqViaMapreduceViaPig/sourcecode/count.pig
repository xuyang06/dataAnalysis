register '/home/cloudera/hadoop-myudfs.jar';
define IdColumnValue myudfs.Parser();
A = Load '/home/cloudera/input2.txt' AS (line:chararray);
B = foreach A generate FLATTEN(IdColumnValue($0)) as (id:chararray, number:int);
C = GROUP B by id;
D = foreach C generate group, SUM(B.number);
dump D;
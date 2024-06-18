-- How to execute(In Terminal):
--~/pig-0.17.0/bin/pig -x local -param M=M-matrix-small.txt
-- -param N=N-matrix-small.txt -param O=output770 multiply.pig

--matR = LOAD 'M-matrix-small.txt' USING PigStorage(',') AS (row,column,value:double);
matR = LOAD '$M' USING PigStorage(',') AS (row,column,value:double);

matS = LOAD '$N' USING PigStorage(',') AS (row,column,value:double);

X = JOIN matR BY column FULL OUTER, matS BY row;
Y = FOREACH X GENERATE matR::row AS mRr, matS::column AS mSc, (matR::value)*(matS::value) AS value;
Z = GROUP Y BY (mRr, mSc);

multiplied_matrix = FOREACH Z GENERATE group.$0 as row, group.$1 as column, SUM(Y.value) AS value;

--
STORE multiplied_matrix INTO '$O' USING PigStorage(',');

--product_output = LOAD '$O/part-r-00000' USING PigStorage(',');
--STORE product_output INTO 'solution_temp.txt' USING PigStorage(',');

--fs -mv solution_temp.txt solution.txt

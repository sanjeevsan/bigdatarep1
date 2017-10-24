load_data = load '/user/hive/warehouse/project.db/h1b_final' using PigStorage('\t') as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);

filter_data1 = filter load_data by job_title == 'DATA ENGINEER' and year == '2011';

group_data1 = group filter_data1 by worksite;

count_data1 = foreach group_data1 generate '2011',group as worksite,COUNT(filter_data1.job_title)as headcount;

orderby_data1 = limit (order count_data1 by headcount desc) 1;

--dump orderby_data;
filter_data2 = filter load_data by job_title == 'DATA ENGINEER' and year == '2012';

group_data2 = group filter_data2 by worksite;

count_data2 = foreach group_data2 generate '2012',group as worksite,COUNT(filter_data2.job_title)as headcount;

orderby_data2 = limit (order count_data2 by headcount desc) 1;

filter_data3 = filter load_data by job_title == 'DATA ENGINEER' and year == '2013';

group_data3 = group filter_data3 by worksite;

count_data3 = foreach group_data3 generate '2013',group as worksite,COUNT(filter_data3.job_title)as headcount;

orderby_data3 = limit (order count_data3 by headcount desc) 1;

filter_data4 = filter load_data by job_title == 'DATA ENGINEER' and year == '2014';

group_data4 = group filter_data4 by worksite;

count_data4 = foreach group_data4 generate '2014',group as worksite,COUNT(filter_data4.job_title)as headcount;

orderby_data4 = limit (order count_data4 by headcount desc) 1;

filter_data5 = filter load_data by job_title == 'DATA ENGINEER' and year == '2015';

group_data5 = group filter_data5 by worksite;

count_data5 = foreach group_data5 generate '2015',group as worksite,COUNT(filter_data5.job_title)as headcount;

orderby_data5 = limit (order count_data5 by headcount desc) 1;

filter_data6 = filter load_data by job_title == 'DATA ENGINEER' and year == '2016';

group_data6 = group filter_data6 by worksite;

count_data6 = foreach group_data6 generate '2016',group as worksite,COUNT(filter_data6.job_title)as headcount;

orderby_data6 = limit (order count_data6 by headcount desc) 1;

final_data = union orderby_data1,orderby_data2,orderby_data3,orderby_data4,orderby_data5,orderby_data6;

dump final_data;

--dump orderby_data1;
--dump orderby_data2;
--dump orderby_data3;
--dump orderby_data4;
--dump orderby_data5;
dump orderby_data6;


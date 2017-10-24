load_data = load '/user/hive/warehouse/project.db/h1b_final' using PigStorage('\t') as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:long,year:chararray,worksite:chararray,longitute:double,latitute:double);

groupby_emp = group load_data by job_title;

count_total = foreach groupby_emp generate group,COUNT(load_data.case_status) as totalcount;

filter_certified = filter load_data by case_status == 'CERTIFIED';

group_certified_filter_data = group filter_certified by job_title;
 
count_certified = foreach group_certified_filter_data generate group,COUNT(filter_certified.case_status) as certifiedcount;

filter_certified_withdrawn = filter load_data by case_status == 'CERTIFIED-WITHDRAWN';

group_certified_withdrawn_filter_data = group filter_certified_withdrawn by job_title;
 
count_certified_withdrawn = foreach group_certified_withdrawn_filter_data generate group,COUNT(filter_certified_withdrawn.case_status) as 
certified_withdrawncount;

join_data = join count_total by $0,count_certified by $0,count_certified_withdrawn by $0;

--dump join_data;

join_data1 = foreach join_data generate $0,$1,$3,$5 ;

successive_rate = foreach join_data1 generate $0,(float)($2+$3)/($1)*100,$1;

final = filter successive_rate by $1 > 70 and $2 > 1000;

final_output = order final by $1 desc;

--dump final_output;

store final_output into '/user/hive/ten1' using PigStorage('\t');


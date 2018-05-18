#1.测试udf函数
create table if not exists test_udf_demo_table
(
    school_id string,
    user_id string,
    user_info string,
    info_list string,
    cnt int
)
stored as parquet;

insert overwrite table test_udf_demo_table
select "s01","u01","{name:'ztwu1',age:25,sex:'男'}","[{name:'ztwu1',age:25,sex:'男'},{name:'ztwu2',age:25,sex:'男'}]",10
union all
select "s01","u02","{name:'ztwu2',age:25,sex:'男'}","[{name:'ztwu1',age:25,sex:'男'},{name:'ztwu2',age:25,sex:'男'}]",20
union all
select "s01","u01","{name:'ztwu3',age:25,sex:'男'}","[{name:'ztwu1',age:25,sex:'男'},{name:'ztwu2',age:25,sex:'男'}]",10
union all
select "s02","u04","{name:'ztwu4',age:25,sex:'男'}","[{name:'ztwu1',age:25,sex:'男'},{name:'ztwu2',age:25,sex:'男'}]",15
union all
select "s02","u05","{name:'ztwu5',age:25,sex:'男'}","[{name:'ztwu1',age:25,sex:'男'},{name:'ztwu2',age:25,sex:'男'}]",10;

add jar /project/edu_edcc_dev/xrding/jars/hive-udf/hive-udf-test.jar;
create temporary function func1 as 'com.iflytek.edmp.UDFDemo';
create temporary function func2 as 'com.iflytek.edmp.UDTFDemo';
create temporary function func3 as 'com.iflytek.edmp.UDAFDemo';

SELECT func1(user_info,"name") from test_udf_demo_table;

SELECT name,age from test_udf_demo_table lateral view func2(info_list)info as name,age;

SELECT school_id,func3(user_id,cnt) from test_udf_demo_table group by school_id;
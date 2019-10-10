CREATE EXTERNAL TABLE mooc.user_course
(uid STRING, nickname STRING, birthdate STRING, gender STRING,
district STRING, last_login_time STRING, term_id STRING, course_id STRING, select_date STRING)
COMMENT 'This is the mooc course select data' ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '/Users/raymond/Documents/大数据教材/user_course.csv';

CREATE EXTERNAL TABLE mooc.user_course
(uid STRING, nickname STRING, birthdate STRING, gender STRING,
district STRING, last_login_time STRING, term_id STRING, course_id STRING, select_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '/Users/raymond/Documents/大数据教材/user_course.csv' OVERWRITE INTO TABLE mooc.user_course;
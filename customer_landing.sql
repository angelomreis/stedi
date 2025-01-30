{\rtf1\ansi\ansicpg1252\cocoartf2821
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fnil\fcharset0 HelveticaNeue;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\deftab560
\pard\pardeftab560\slleading20\partightenfactor0

\f0\fs26 \cf0 CREATE EXTERNAL TABLE IF NOT EXISTS `udacity-db`.`customer_landing` (\
  `customerName` string,\
  `email` string,\
  `phone` string,\
  `birthDay` string,\
  `serialNumber` string,\
  `registrationDate` bigint,\
  `lastUpdateDade` bigint,\
  `shareWithResearchAsOfDate` bigint,\
  `shareWithPublicAsOfDate` bigint,\
  `shareWithFriendsAsOfDate` bigint\
)\
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\
WITH SERDEPROPERTIES (\
  'ignore.malformed.json' = 'FALSE',\
  'dots.in.keys' = 'FALSE',\
  'case.insensitive' = 'TRUE',\
  'mapping' = 'TRUE'\
)\
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\
LOCATION 's3://udacity-bkt/customer/landing/'\
TBLPROPERTIES ('classification' = 'json');}
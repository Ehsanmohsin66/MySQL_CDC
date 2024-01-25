## CDC for SCD type 2 dimension from MySQL table

# Dependencies

Following python libararies are 
- pandas (Version: 1.1.5)
- pyarrow (12.0.1)
- s3fs (2023.1.0)
- kafka (1.3.5)

Parquet file format is used as it is good for analytical queries.

# SETUP

Following installations are done on local computer
- MySQL Server and client
- Zookeeper (Docker container)
- Kafka (Docker container)
- Debezium Kafka Connect (Docker container)
- MySQL Debezium connector (Connector API)
- Mock S3 (localstack container with s3)
- Enable MySQL binlog by adding following line in mysql configuration
server_id         = 1,
log_bin           = mysql-bin,
binlog_format     = ROW,
expire_logs_days  = 10,
binlog_row_image  = FULL,

# CDC

Change Data Capture is mechanism of extracting change in source such as MySQL database.
In a MySQL table changes can happen through Alter, Update, Delete and Insert commands.
In this code i have only captured Update, Delete and Insert commands. Exact row determined is through
Primary Key of table. So pre-requisite for CDC on MySQL table is Primary Key.

# SCD Type 2

Slowly Changing Dimension Type 2 is a common technique to preserve history in a dimension table.
History can be preserved by inserting new row for every change and marking old row inactive.
Inactive rows can be marked using a boolean flag such as the ACTIVE_RECORD column set to 'F' or a start and end date.
I have used flag technique rather than start and end date.

# Methodology

Debezium is configured to read mysql-binlog files and publish change to a pre-configured topic i.e. cdc_test_finalto
Format of key of every new record in this kafka topic will indicate Primary keys and their in row changed.
Example of Key of debezium output record
{ "schema":
{ "type":"struct", 
"fields":[ { "type":"int", "optional":None, "field":"id" } ], 
"optional":None, "name":"test_finalto.customer.Key" }, 
**"payload":{ "id":1 }** 
}

In this key.Payload you can find all key columns and their values.

Value of every message in kafka topic consists of schema and payload. 
I have not used value.schema though it can be used in future.
An example payload is shown below
{"schema":{}, "payload": 
{ "**before**": None, 
"**after**": {  "id": 1, "first_name": "Rob", "last_name": "Key", "Address":"39 Mayfair London", "total_purchase":123.67,"number_orders":1 }, 
"source": {  "name": "2.5.0.Final", "name": "dbmysql", "server_id": 1, "ts_sec": 1986502358, "gtid": None, "file": "mysql-bin.000001",
"pos": 72, "row": 0, "snapshot": None, "thread": 3, "db": "test_finalto", "table": "customer" },
**"op": "i"**,  "ts_ms": 1986502358 } }

value.payload.op field value will determine whether the row was **i**nserted, **u**pdated or **d**eleted
New row to inserted in dimension table is as per value.payload.after field. An additional field of active_record is added
to this after dictionary and added to pandas dataframe.




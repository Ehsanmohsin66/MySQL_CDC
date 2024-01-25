import pandas as pd
import pyarrow.parquet as pq
import s3fs

from json import loads
from pandas import DataFrame
from kafka import KafkaConsumer


def load_dimension(db_name: str, table_name: str):
    s3 = s3fs.S3FileSystem()
    old_pandas_dataframe = pq.ParquetDataset(f's3://{db_name}/{table_name}/', filesystem=s3).read_pandas().to_pandas()
    return old_pandas_dataframe


def upload_dimension(data_frame: DataFrame, key_columns: [], db_name: str, table_name: str):
    s3_url = f's3://{db_name}/{table_name}/'
    data_frame.to_parquet(s3_url,
                          engine='pyarrow',
                          partition_cols=key_columns,
                          existing_data_behavior='delete_matching')


def cdc_scd2(db_name: str, table_name: str, kafka_topic: str, kafka_ser: str):
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=[kafka_ser],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='cdc_group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))
    for rec in consumer:
        message_payload = rec.value
        message_key = rec.key
        table_name_inp = message_payload["payload"]["source"]["table"]
        db_name_inp = message_payload["payload"]["source"]["db"]
        if db_name_inp == db_name and \
                table_name_inp == table_name:
            operation = message_payload["payload"]["op"]
            try:
                data_df = load_dimension(db_name, table_name)
            except:
                # only make empty df if after is present
                if operation != 'd':
                    lst = list(message_payload["payload"]["after"].keys())
                    lst.append('active_record')
                    data_df = pd.DataFrame(columns=lst)
                else:
                    break
            pk_condition = True
            for key, value in message_key["payload"].items():
                pk_condition = pk_condition and data_df[key] != value
            if operation == 'd':
                data_df['active_record'] = data_df['active_record'].where(pk_condition, 'F')
            elif operation == 'i':
                new_record = message_payload["payload"]["after"]
                new_record['active_record'] = 'T'
                new_record_df = pd.DataFrame([new_record])
                data_df = pd.concat([data_df, new_record_df], ignore_index=True)
            elif operation == 'u':
                data_df['active_record'] = data_df['active_record'].where(pk_condition, 'F')
                new_record = message_payload["payload"]["after"]
                new_record['active_record'] = 'T'
                new_record_df = pd.DataFrame([new_record])
                data_df = pd.concat([data_df, new_record_df], ignore_index=True)
            else:
                print("operation is null")

            upload_dimension(data_df, list(message_key["payload"].keys()), db_name, table_name)

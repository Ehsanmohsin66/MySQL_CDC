from kafka_consumer_cdc import cdc_scd2

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    kafka_uri = 'localhost:9092'
    kafka_topic = 'cdc_test_finalto'
    db_name = 'test_finalto'
    table_name = 'customer'
    new_pandas_dataframe=cdc_scd2(db_name, table_name, kafka_topic, kafka_uri)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

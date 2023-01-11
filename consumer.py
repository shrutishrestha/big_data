from kafka import KafkaConsumer

if __name__ == "__main__":
    print("Kafka Consumer Application Started ... ")

    try:
        consumer = KafkaConsumer("registered_user", bootstrap_servers="localhost:9092", auto_offset_reset="earliest",group_id="consumer group a");
        print("Consumer data...")
        for data in consumer:
            item = data.value
            print("Consumer data are:... ", item)
    except Exception as exception:
        print("Failed to read kafka message.")
        print(exception)

    print("Kafka Consumer Application Completed. ")
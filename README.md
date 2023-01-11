# big_data

The NASA API was hit and the images are downloaded. The downloaded images are kept in a folder for processing. During processing, the parameters are calculated from the
Apache Kafka producer. Then, Apache Kafka is used as an intermediate buffer that can hold the message generated from the sources before going to the processing layer. The use of
Kafka here as a buffer helps playback any delay in data generation and processing on the operational layer. The events from Kafka are then consumed from Spark Structured Streaming.
The consumed data is transformed and pre-processed before generating aggregated data. Once the data is ready, we push the data to the PostgreSQL database table.


# About the project
The aim of the project was to install a throatle service so that it won't be a bottleneck in the RDBMS (postgres database) while storing the calculated data during fetching and continously calculating the parameters. 

![Screen Shot 2023-01-11 at 12 53 33 AM](https://user-images.githubusercontent.com/33342277/211728730-7cd4776c-7bfb-466f-b0ae-c580aae7cb26.png)

The approach we followed was: 


![Screen Shot 2023-01-11 at 12 53 51 AM](https://user-images.githubusercontent.com/33342277/211728774-ffa59309-43ac-4ed4-9faa-8f9fbc8ca8ba.png)



The NASA API is hit and the images are downloaded. The downloaded images are kept in a folder for processing. During processing, the parameters are calculated from the Apache Kafka producer. Then, Apache Kafka is used as an intermediate buffer that can hold the message generated from the sources before going to the processing layer. The use of Kafka here as a buffer helps playback any delay in data generation and processing on the operational layer. The events from Kafka are then consumed from Spark Structured Streaming. The consumed data is transformed and pre-processed before generating aggregated data. Once the data is ready, we push the data to the PostgreSQL database table in specific time interval.


**Main files:** 

producer.py : reads the data, transposes, applies parallelize RDD, calculates the parameters with map and collect libraries, then sends it to producer object.

consumer.py: once the producer object starts producing data, it can be seen in the consumer terminal. It does not store the data but its just for our visualizing purpose.

real_time_data_streaming.py:  This file has the program to connect and write in postgresql. It waits for allocated processingTime (10 secs in this case), holds data for 10 secs, and writes all the data collected within the interval of 10 sec to postgres at one shot.


**Running commands:**

Terminal 1: python producer.py 

Terminal 2: python consumer.py 

Terminal 3: python real_time_data_streaming.py 

**Demo**
: https://drive.google.com/file/d/12DJ6RDZlu2irHqLe-fRk1W_Nyc0158H9/view?usp=share_link

**Result**
producer:
![Screen Shot 2023-01-11 at 1 17 56 AM](https://user-images.githubusercontent.com/33342277/211732311-6ae28baf-d7b7-4014-a950-fcf09c41da9f.png)

consumer: 
![Screen Shot 2023-01-11 at 1 17 17 AM](https://user-images.githubusercontent.com/33342277/211732364-e4ccef69-df8e-4619-b1ce-b1b230d5668f.png)

real_time_data_streaming:
![Screen Shot 2023-01-11 at 1 17 33 AM](https://user-images.githubusercontent.com/33342277/211732410-9968f76e-8834-4680-b992-63a2b80595b3.png)

postgres database:
![Screen Shot 2023-01-11 at 1 16 54 AM](https://user-images.githubusercontent.com/33342277/211732451-7319ab69-e2c7-4754-b274-eb5ea3fde595.png)




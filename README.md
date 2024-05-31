# streaming-05-smart-smoker
Module 5 of Streaming Data: Creating a Producer
Author: Levi Lowther
Created: 27May2024

## A Smart Smoker that Monitors Temperature
> In this module we create a producer of messages that reads from a CSV.
> These messages are read and sent to a queue that will be read later on.  

## Requirements
 > RabbitMQ server running pika installed in your active environment RabbitMQ Admin See http://localhost:15672/Links to an external site.
 > Python 3.12

## Guided Producer Design
1. If this is the main program being executed (and you're not importing it for its functions),
2. We should call a function to ask the user if they want to see the RabbitMQ admin webpage.
3. We should call a function to begin the main work of the program.
4. As part of the main work, we should
    1. Get a connection to RabbitMQ, and a channel, delete the 3 existing queues (we'll likely run this multiple times), and then declare them anew. 
    2. Open the csv file for reading (with appropriate line endings in case of Windows) and create a csv reader.
    3. For data_row in reader:
        1. [0] first column is the timestamp - we'll include this with each of the 3 messages below
        2. [1] Channel1 = Smoker Temp --> send to message queue "01-smoker"
        3. [2] Channe2 = Food A Temp --> send to message queue "02-food-A"
        4. [3] Channe3 = Food B Temp --> send to message queue "02-food-B"
        5. Send a tuple of (timestamp, smoker temp) to the first queue
        6. Send a tuple of (timestamp, food A temp) to the second queue
        7. Send a tuple of (timestamp, food B temp) to the third queue 
        8. Create a binary message from our tuples before using the channel to publish each of the 3 messages.
5. Messages are strings, so use float() to get a numeric value where needed
6. Remember to use with to read the file, or close it when done.

## Screenshot of Producer Working
![Producer working](BBQ-Producer.png)
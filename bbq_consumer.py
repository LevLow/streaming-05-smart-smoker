"""
Levi Lowther 05June2024
    This program listens for work messages contiously. 
   
    Author: Levi Lowther
    Date: 05June2024

"""

import pika
import sys
import time
import os
import time
import csv
import webbrowser
import traceback
from collections import deque
from datetime import datetime
from util_logger import setup_logger

#setup logging

from util_logger import setup_logger

logger, logname = setup_logger(__file__)

# Define Variables for our smoker checks. Time windows for smoker temp is 2.5 minutes and time window for food temp is 10 minutes. 

SMOKER_MAXLEN = 5 #one reading every 30 seconds (2.5 min * 1 reading/0.5)
FOOD_MAXLEN = 20 #one reading every 30 seconds (10 min * 1 reading/ 0.5)

SMOKER_ALERT_THRESHOLD = 15.0 #degrees fahrenheit (if smoker temp decreases more than or equal to 15 degrees in 2.5 min)
FOOD_STALL_THRESHOLD = 1.0 #degree farenheit (if food temp changes 1 degree or less in 10 minutes)

smoker_temps = deque(maxlen=SMOKER_MAXLEN)
food_A_temps = deque(maxlen=FOOD_MAXLEN)
food_B_temps = deque(maxlen=FOOD_MAXLEN)

# define a fucntion to check the temperature of the smoker against the threshold
def check_smoker_temp():
    """Check if smoker temperature has changed by more than or equal to the given threshold in the current time window"""
    if len(smoker_temps) == SMOKER_MAXLEN:
        initial_temp = smoker_temps[0][1]
        latest_temp = smoker_temps[-1][1]
        if initial_temp - latest_temp >= SMOKER_ALERT_THRESHOLD:
            alert_message = f"Smoker Alert! Temperature dropped by {initial_temp-latest_temp}°F within the last 2.5 minutes!"
            logger.info(alert_message)

def check_food_temp(deque, food_name):
    """Check if food temperature has changed by less than or equal to the given threshold within the latest time window"""
    if len(deque) == FOOD_MAXLEN:
        initial_temp = deque[0][1]
        latest_temp = deque[-1][1]
        if abs(initial_temp - latest_temp) <= FOOD_STALL_THRESHOLD:
            alert_message = f"Food Stall Alert! {food_name} temperature has changed by {abs(initial_temp - latest_temp)}°F in the last 10 minutes!"
            logger.info(alert_message)


# define a callback function to be called when a message is received
def callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    message = eval(body.decode())
    timestamp, temp = message
    timestamp = datetime.strptime(timestamp, '%m/%d/%y %H:%M:%S')
    
    if method.routing_key == "01-smoker":
        smoker_temps.append((timestamp, temp))
        check_smoker_temp()
    elif method.routing_key == "02-food-A":
        food_A_temps.append((timestamp, temp))
        check_food_temp(food_A_temps, "Food A")
    elif method.routing_key == "02-food-B":
        food_B_temps.append((timestamp, temp))
        check_food_temp(food_B_temps, "Food B")
    
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# define a main function to run the program
def main():
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))

    # except, if there's an error, do this
    except Exception as e:
        logger.error("ERROR: connection to RabbitMQ server failed.")
        logger.error(f"Verify the server is running on host={"localhost"}.")
        logger.error(f"The error says: {e}")
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        queues = ["01-smoker", "02-food-A", "02-food-B"]
        for queue_name in queues:
            channel.queue_declare(queue=queue_name, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        for queue_name in queues:
            channel.basic_consume(queue=queue_name, on_message_callback=callback)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logger.error("ERROR: something went wrong.")
        logger.error(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.error(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logger.error("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main()

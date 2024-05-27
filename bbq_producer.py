""""
Levi Lowther
27May2024
Producer of messages for a Smart Smoker BBQ system

"""

import csv
import pika
import sys
import webbrowser

#setup logger

from util_logger import setup_logger

logger, logname = setup_logger(__file__)

# define fuctions
def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    show_offer = False
    if show_offer:
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        logger.info(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        logger.info(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

# Read tasks from csv and send to RabbitMQ server
def read_and_send_tasks_from_csv(file_path: str, host: str, queue_name: str):
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader: 
            message = " ".join(row)
            send_message(host, queue_name, message)


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()
    # get the message from the command line
    # if no arguments are provided, use the default message
    # use the join method to convert the list of arguments into a string
    # join by the space character inside the quotes
    # define the name of the file 
    file_name = 'tasks.csv'
    #define host
    host = 'localhost'
    #define queue name
    queue_name = 'task_queue3'
    # send the message to the queue
    read_and_send_tasks_from_csv(file_name, host, queue_name)

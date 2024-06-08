
""""
Levi Lowther
27May2024
Producer of messages for a Smart Smoker BBQ system

""" 
import pika
import sys
import webbrowser
import csv
import struct
import time 
from datetime import datetime
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

# use to control whether or not admin page is offered to user.
# change to true to receive offer, false to remove prompt
show_offer = True

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    if show_offer:
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()


def main():
    """
    Read a CVS, row by row. Send messages based on the queue the info is coming from 
    """
   
    offer_rabbitmq_admin_site()

    logger.info(f'Attempting to access smoker-temps.csv')

    try:
        # access file
        with open("smoker-temps.csv", newline='') as csvfile:
            reader = csv.reader(csvfile)

            # Skip header row
            next(reader)

            # Main section of code where data is reviewed and messages are sent.
            for row in reader:

                # assign variables from row
                string_timestamp = row[0]
                smoker_Temp = row[1]
                food_A_Temp = row[2]
                food_B_Temp = row[3]

                               
                # convert datetime string into a datetime object
                datetime_timestamp = datetime.strptime(string_timestamp, "%m/%d/%y %H:%M:%S").timestamp()
                
                #check for value and send to correct queue
                if smoker_Temp:
                    # pack message contents into serialized format
                    message = struct.pack("!df", datetime_timestamp, float(smoker_Temp))
                    send_message('localhost', '01-smoker', message)

                # food A
                if food_A_Temp:
                    # pack message contents into serialized format
                    message = struct.pack("!df", datetime_timestamp, float(food_A_Temp))
                    send_message('localhost', '02-food-A', message)

                # food B
                if food_B_Temp:
                    # pack message contents into serialized format
                    message = struct.pack("!df", datetime_timestamp, float(food_B_Temp))
                    send_message('localhost', '03-food-B', message)

                time.sleep(.05)

                
    except Exception as e:
        logger.info(f'ERROR: {e}')

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

       """

    try:
        logger.info(f"send_message({host=}, {queue_name=}, {message=})")
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))

        # use the connection to create a communication channel
        ch = conn.channel()
        logger.info(f"connection opened: {host=}, {queue_name=}")

        # declare a durable queue
        ch.queue_declare(queue=queue_name, durable=True)

        #publish a message to the queue
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)

        # print a message to the console for the user
        logger.info(f" [x] Sent {message}")

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
        
    finally:
        # close the connection to the server
        conn.close()
        logger.info(f"connection closed: {host=}, {queue_name=}")
        

# If this is the program being run, then execute the code below
if __name__ == "__main__":  

    # specify file path for data source
    file_path = 'smoker-temps.csv'

    # transmit task list
    logger.info(f'Beginning process: {__name__}')
    main()
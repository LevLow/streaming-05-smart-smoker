""""
Levi Lowther
27May2024
Producer of messages for a Smart Smoker BBQ system

"""

import csv
import pika
import sys
import webbrowser
import pathlib
from util_logger import setup_logger


logger, logname = setup_logger(__file__)

# define fuctions
def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    show_offer = True
    if show_offer:
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

def conn_q_csv():
    """
    this will create our connection to RabbitMQ, Delete existing queues, and make new queues.
    This will also read our CSV file. 
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        # use the connection to create a communication channel
        ch = conn.channel()

        #declare a new variable name queues with our queue names and a 
        # function to delete old and make new queues
        queues = ["01-smoker", "02-food-A", "02-food-B"]
        for queue_name in queues:
            ch.queue_delete(queue=queue_name)
            ch.queue_declare(queue=queue_name, durable=True)


        # Process CSV and send messages to queues
        file_path = "/Users/levilowther/ds-venv/streaming-05-smart-smoker/smoker-temps.csv"
        with open(file_path, newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
        
            # define data in CSV
            for row in reader:
                timestamp = row['Time (UTC)']
                smoker_temp_str = row['Channel1']
                food_a_temp_str = row['Channel2']
                food_b_temp_str = row['Channel3']

                if smoker_temp_str:
                    smoker_temp = float(smoker_temp_str)
                    send_message(ch, "01-smoker", (timestamp, smoker_temp))
                if food_a_temp_str:
                    food_a_temp = float(food_a_temp_str)
                    send_message(ch, "02-food-A", (timestamp, food_a_temp))
                if food_b_temp_str:
                    food_b_temp = float(food_b_temp_str)
                    send_message(ch, "02-food-B", (timestamp, food_b_temp))
    except FileNotFoundError:
        logger.error("CSV file not found.")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Error processing CSV: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)
    finally:
        conn.close
        

def send_message(channel: str, queue_name: str, message: str):
    """
    Publish a message to the specified queue.

    Parameters:
        queue_name (str): The name of the queue
        message (tuple): The message to be sent to the queue
    """
    try:
        channel.basic_publish(exchange="", routing_key=queue_name, body=str(message))
        logger.info(f"Sent message to {queue_name}: {message}")
    except Exception as e:
        logger.error(f"Error sending message to {queue_name}: {e}")
  

if __name__ == "__main__":
    offer_rabbitmq_admin_site()
    conn_q_csv()



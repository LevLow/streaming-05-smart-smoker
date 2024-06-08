""""
Levi Lowther
03June2024
Consumer of messages for a Smart Smoker BBQ system

""" 
import pika
import sys
import time
import struct
from datetime import datetime
from collections import deque
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

# create deques 
smoker_temps = deque(maxlen = 5) # 2.5 min * 1 reading/0.5 min
food_A_temps = deque(maxlen = 20) # 10 min * 1 reading/0.5 min
food_B_temps = deque(maxlen = 20) # 10 min * 1 reading/0.5 min

# define callbacks for each queue when called
# smoker callback
def smoker_callback(ch, method, properties, body):
    """ 
    smoker queue callback function
    """
    # unpack struct of bbq_producer
    timestamp, temperature = struct.unpack('!df', body) 

    #round Temp
    round_temp = round(temperature, 2)

    # convert timestamp back to string
    timestamp_str = datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S")
    logger.info(f' [Smoker Temp Check]: {timestamp_str}: {round_temp}°F')

    # Add new temperature
    smoker_temps.append(temperature)

    # check for chance in temp once deque is at maxlen
    if len(smoker_temps) == smoker_temps.maxlen:
        if temp_change_cacl(smoker_temps) <= -15:
            logger.info(f'''
Smoker Temp Alert! Temperature has changed 15°F or more in the last 2.5 minutes!
Time of Alert: {timestamp_str}
Temp at Time of Alert: {round_temp}''')
       
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Food A call back 
def food_A_callback(ch, method, properties, body):
    """ 
    Food A queue callback function 
    """
    # unpack info from bbq_producer
    timestamp, temperature = struct.unpack('!df', body) 

    #round Temp
    round_temp = round(temperature, 2)
    
    # convert timestamp to string
    timestamp_str = datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S")
    logger.info(f' [Food A Temp Check]: {timestamp_str}: {round_temp}°F')

    # Add new temperature to deque
    food_A_temps.append(round_temp)

    # Once the deque is full, check for a temperature drop
    if len(food_A_temps) == food_A_temps.maxlen:
        if temp_change_cacl(food_A_temps) <= 1:
            logger.info(f''' 
Food A Stall Alert! Temperature has changed < 1°F in the last 10 minutes!
Time of Alert: {timestamp_str}
Temp at Time of Alert: {temperature}''')

       
    ch.basic_ack(delivery_tag=method.delivery_tag)

#food B callback
def food_B_callback(ch, method, properties, body):
    """ 
    Food B queue callback function
    """
    # unpack struct sent by emmiter_of_tasks.py
    timestamp, temperature = struct.unpack('!df', body) # timestamp will only be used for logging

     #round Temp
    round_temp = round(temperature, 2)

    # convert timestamp back to a string for logging
    timestamp_str = datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S")
    logger.info(f' [Food B Temp Check]: {timestamp_str}: {round_temp}°F')

    # Add new temperature to deque
    food_B_temps.append(temperature)

    # Once the deque is full, check for a temperature drop
    if len(food_B_temps) == food_B_temps.maxlen:
        if temp_change_cacl(food_B_temps) <= 1:
            logger.info(f'''
Food B Stall Alert! Temperature has changed < 1°F in the last 10 minutes!
Time of Alert: {timestamp_str}
Temp at Time of Alert: {round_temp}''')

    
    ch.basic_ack(delivery_tag=method.delivery_tag)

# function to calculate the change in temperature
def temp_change_cacl(collection):
    '''calcluates difference between last item and fist item'''
    return collection[-1] - collection[0]


# define a main Function
def main(hn: str = "localhost"):
    """ Continuously listen for messages"""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # Declare queues
        queues = ('01-smoker', '02-food-A', '03-food-B')

        # create durable queue for each queue
        for queue in queues:
            # delete queue if it exists
            channel.queue_delete(queue=queue)
            # create new durable queue
            channel.queue_declare(queue=queue, durable=True)

        # restrict worker to one unread message at a time
        channel.basic_qos(prefetch_count=1) 

        # listen to each queue and execute corresponding callback function
        channel.basic_consume( queue='01-smoker', on_message_callback=smoker_callback)
        channel.basic_consume( queue='02-food-A', on_message_callback=food_A_callback)
        channel.basic_consume( queue='03-food-B', on_message_callback=food_B_callback)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost")
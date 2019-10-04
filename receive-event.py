import os
import sys
import logging
import time
from config import eventhubconfig as eventcfg
# Import classes from Event Hubs python package
from azure.eventhub import EventHubClient, Receiver, Offset, EventData
from datetime import datetime
# datetime object containing current date and time.
now = datetime. now()
print("now =", now)
# dd/mm/YY H:M:S.
dt_string = now. strftime("%d/%m/%Y %H:%M:%S")
print("date and time =", dt_string)

# Address can be in either of these formats:
# "amqps://<URL-encoded-SAS-policy>:<URL-encoded-SAS-key>@<mynamespace>.servicebus.windows.net/myeventhub"
# "amqps://<mynamespace>.servicebus.windows.net/myeventhub"
# For example:
#ADDRESS = "amqps://<MyEventHubNamspaceName>.servicebus.windows.net/<MyEventHubName>"
ADDRESS = eventcfg.address


# SAS policy and key are not required if they are encoded in the URL
USER = eventcfg.user
KEY = eventcfg.key

CONSUMER_GROUP = "$default"
OFFSET = Offset("-1")
PARTITION = "0"

total = 0
last_sn = -1
last_offset = "-1"
client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)
try:
    receiver = client.add_receiver(
        CONSUMER_GROUP, PARTITION, prefetch=5000, offset=OFFSET)
    client.run()
    start_time = time.time()
    print("[")
    index=0
    for event_data in receiver.receive(timeout=1000):
        index+=1
        if index==0:
            print('"Received":{}'.format(event_data.body_as_str(encoding='UTF-8')))
        else:
            print(',"Received":{}'.format(event_data.body_as_str(encoding='UTF-8')))
        total += 1
    print("]")
    end_time = time.time()
    client.stop()
    run_time = end_time - start_time
    print("Received {} messages in {} seconds".format(total, run_time))

except KeyboardInterrupt:
    pass
finally:
    client.stop()
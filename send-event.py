# Import classes from Event Hubs python package
from azure.eventhub import EventHubClient, Receiver, Offset, EventData
from datetime import datetime
from config import eventhubconfig as eventcfg
import json
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

# Create an Event Hubs client
client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)

# Add a sender to the client
sender = client.add_sender(partition="0")

# Run the Event Hub client
client.run()
#dt_string= '"'+dt_string + '"'
# Send event to the event hub
json_data = [{
   "id":dt_string,
   "eventTime": dt_string,
   "eventType": "test",
   "data": {
    "resourceUri": "test",
    "operationName": "test",
    "performance_Value": "test",
    "resourceProvider": "test",
    "status": "test"
   },
    "subject": "test"
}]

eventMessage = json.dumps(json_data)
print (eventMessage)
sender.send(EventData(eventMessage))

# Stop the Event Hubs client
client.stop()
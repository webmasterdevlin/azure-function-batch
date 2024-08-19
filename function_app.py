import azure.functions as func
import logging
import json

from message_bus import MessageBus

app = func.FunctionApp()

receive_queue_name = "batch_queue"
send_queue_name = "auto_scaler_queue"
access_rights="listen"

message_bus = MessageBus()
message_bus.connect()

@app.service_bus_queue_trigger(
    arg_name="msg", 
    queue_name=receive_queue_name, 
    connection="ServiceBusConnection", 
    access_rights=access_rights)
def servicebus_queue_trigger(msg: func.ServiceBusMessage):
    body_str = msg.get_body().decode("utf-8")
    input_data = json.loads(body_str)
    logging.info(f"Receiving {input_data} from {receive_queue_name}")
    logging.info(f"Sending {input_data} to {send_queue_name}")

    message_bus.send(queue=send_queue_name, msg=json.dumps(input_data))


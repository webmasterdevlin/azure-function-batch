import os
import azure.functions as func
import logging
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusReceivedMessage, ServiceBusReceiveMode
from azure.identity import DefaultAzureCredential
from functools import wraps
from typing import Callable, Optional

fully_qualified_namespace = "centauri-message-broker.servicebus.windows.net"

class MessageBus:
    """
    A class to manage message bus operations using Azure Service Bus.
    """

    def __init__(self):
        self.service_callback_handler: Optional[Callable[[ServiceBusReceivedMessage], None]] = None
        self.servicebus_client: Optional[ServiceBusClient] = None
        self.timers = {}  # Dictionary to keep track of timers for each message

    def _reconnect_if_required(func: Callable) -> Callable:
        """
        A decorator to handle reconnection if the connection to the service bus is lost.
        """

        @wraps(func)
        def _func_wrapper(*args, **kwargs):
            self = args[0]
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"Error: {e}. Attempting to reconnect...")
                self.connect()
                return func(*args, **kwargs)
        return _func_wrapper

    def connect(self) -> None:
        """
        Connect to Azure Service Bus using Azure Identity.
        """
        try:
            credential = DefaultAzureCredential()
            self.servicebus_client = ServiceBusClient(
                fully_qualified_namespace=fully_qualified_namespace,
                credential=credential
            )
            print("Connected to Azure Service Bus")
        except Exception as e:
            print("Could not connect to the Azure Service Bus")
            raise

    def _renew_lock_periodically(self, receiver, message):
        def renew_lock():
            try:
                receiver.renew_message_lock(message)
                print("Lock renewed for message:", message.message_id)
            except Exception as e:
                print("Failed to renew lock:", e)
        
        def periodic_renewal():
            renew_lock()
            timer = Timer(240, periodic_renewal)  # Renew every 4 minutes (240 seconds)
            self.timers[message.message_id] = timer
            timer.start()
        
        periodic_renewal()

    def _stop_timer(self, message_id):
        """
        Stop the timer for the given message ID.
        """
        timer = self.timers.pop(message_id, None)
        if timer:
            timer.cancel()
            print(f"Timer stopped for message: {message_id}")

    @_reconnect_if_required
    def make_queue_durable(self, queue: str) -> None:
        print(f"Ensuring queue {queue} is durable (this is handled via Azure configurations).")

    @_reconnect_if_required
    def send(self, queue: str, msg: str, correlation_id: Optional[str] = None) -> None:
        self.make_queue_durable(queue)
        with self.servicebus_client.get_queue_sender(queue_name=queue) as sender:
            service_bus_message = ServiceBusMessage(msg, message_id=correlation_id)
            sender.send_messages(service_bus_message)
            print(f"Message sent to queue {queue}")

    @_reconnect_if_required
    def start_consuming(self, queue: str,
                        service_callback_handler: Callable[[ServiceBusReceivedMessage], None]) -> None:
        self.make_queue_durable(queue)
        self.service_callback_handler = service_callback_handler
        with self.servicebus_client.get_queue_receiver(queue_name=queue,
                                                       receive_mode=ServiceBusReceiveMode.PEEK_LOCK) as receiver:
            print(f"Waiting for messages from {queue}...")
            for msg in receiver:
                try:
                    self._renew_lock_periodically(receiver, msg)
                    self.service_callback_handler(msg)
                    receiver.complete_message(msg)
                    self._stop_timer(msg.message_id)  # Stop the timer once the message is processed
                except Exception as e:
                    print(f"Message processing failed: {e}")
                    receiver.abandon_message(msg)
                    self._stop_timer(msg.message_id)  # Stop the timer in case of failure

    def stop(self) -> None:
        self.servicebus_client.close()
        print("Service bus client closed")
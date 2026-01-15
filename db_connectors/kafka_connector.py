from typing import Any, Dict, Literal, Optional , Callable

from pydantic import SecretStr
from .base import Connector
from quixstreams import Application
from quixstreams.kafka import ConnectionConfig , AutoOffsetReset
from functools import wraps


def requires_connection(method):
    """
    Decorator to ensure app and dataframes are initialized before method execution.
    Checks that the KafkaStreamConsumer instance has both 'app' and 'dataframes' initialized.
    
    Usage:
        @requires_connection
        def my_method(self, ...):
            # method implementation
    
    Raises:
        RuntimeError: If app or dataframes are not initialized.
    """
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if not getattr(self, 'app', None):
            raise RuntimeError("Application not initialized. Call connect() first.")
        if not getattr(self, 'dataframes', None):
            raise RuntimeError("Dataframes not initialized. Call connect() first.")
        return method(self, *args, **kwargs)
    return wrapper


class KafkaStreamConsumer(Connector):
    """A connector for Kafka that extends the base Connector class.
    It provides methods to connect, disconnect, check connection status, and retrieve connection information.

    Attributes:
        address (str): The Kafka broker address.
        port (int): The port number for the Kafka broker.
        topic (str): The Kafka topic to connect to.
        consumer_group (str): The consumer group for the Kafka connection.
        auto_offset_reset (str): The offset reset policy, default is 'earliest'.
        security_protocol (str): The security protocol to use, default is 'plaintext'.
        username (str, optional): The username for authentication, if required.
        password (str, optional): The password for authentication, if required.

    Methods:
        connect(): Connects to the Kafka broker and initializes the application and topic.
        disconnect(): Disconnects from the Kafka broker.
        is_connected(): Checks if the connection to the Kafka broker is active.
        get_connection_info(): Returns a dictionary with connection information.
    """

    def __init__(
        self,
        username: Optional[str],
        password: Optional[SecretStr],
        address: str,
        port: int,
        consumer_group: str,
        auto_offset_reset: AutoOffsetReset = "earliest",
        security_protocol: Literal['plaintext', 'ssl', 'sasl_plaintext', 'sasl_ssl'] = "plaintext",
    ):
        super().__init__(address, port)
        self.consumer_group = consumer_group
        self.auto_offset_reset = auto_offset_reset
        self.security_protocol: Literal['plaintext', 'ssl', 'sasl_plaintext', 'sasl_ssl'] = security_protocol
        self.username = username
        self.password = password
        self.dataframes = {}

    def connect(self):
        # Implementation for connecting to Kafka
        print("Connecting to Kafka...")
        try:
            # Add connection logic here
            self.connectionConfig = ConnectionConfig(
                bootstrap_servers=f"{self.address}:{self.port}",
                security_protocol=self.security_protocol,
                sasl_username=self.username,
                sasl_password=self.password,
            )

            self.app = Application(
                broker_address=self.connectionConfig,
                consumer_group=self.consumer_group,
                auto_offset_reset=self.auto_offset_reset,
            )
            #self.topics = []
             
        except Exception as e:
            print(f"Failed to connect to Kafka: {str(e)}")
            raise e
     
    @requires_connection   
    def add_topic(self, new_topic: str):
        """
        Function for adding new topic to consume from Kafka.
        
        :param self: self
        :param new_topic: new topic to add
        :type new_topic: str
        """
        try:
           
            
            topic_obj = self.app.topic(new_topic, value_deserializer="json") 
            df = self.app.dataframe(topic=topic_obj)
          
            
            self.dataframes[new_topic] = df
            print(f"Added topic {new_topic} to consume from Kafka.")
        except Exception as e:
            print(f"Failed to add topic {new_topic} to Kafka: {str(e)}")
            raise e
    
    @requires_connection
    def apply_processing(self, topic: str, processing_function: Callable[[Any], Any]):
        """
        apply processing function to the dataframe of the specified topic.
        
        :param self: self
        :param topic: topic to apply processing function
        :type topic: str
        :param processing_function: The processing function to apply to the dataframe.
        """
        try:
            if topic not in self.dataframes:
                raise RuntimeError(f"datataframes not initialized or Topic {topic} is not added. Call connect() and add_topic() first.")
            
            df = self.dataframes[topic]
            processed_df = processing_function(df)
            self.dataframes[topic] = processed_df
            print(f"Applied processing function to topic {topic}.")
        except Exception as e:
            print(f"Failed to apply processing to topic {topic}: {str(e)}")
            raise e
        
    #WARNING: This method will be deprecated in future versions 
    @requires_connection  
    def consume(self, topic ):
        
        try: 
            # if not self.app:
            #     raise RuntimeError("Application is not initialized. Call connect() first.")
            
            self.topic_obj = self.app.topic(topic, value_deserializer="json")
            self.sdf_stream = self.app.dataframe(topic=self.topic_obj)
            print("Consuming from Kafka topic:", topic)       
        except Exception as e:
            print(f"Failed to consume from Kafka topic {topic}: {str(e)}")
            raise e
    
    @requires_connection
    def sink_to_topic(self, source_topic: str, output_topic: str, value_serializer="json"):
        """Sink processed dataframe to output topic."""
        if source_topic not in self.dataframes:
            raise RuntimeError(f"Topic {source_topic} not added.")
        
        output_topic_obj = self.app.topic(output_topic, value_serializer=value_serializer)
        self.dataframes[source_topic].sink(output_topic_obj)
        print(f"Configured sink: {source_topic} → {output_topic}")

    @requires_connection        
    def run(self):
        try:

            
            self.app.run()
            print("Kafka application started.")
        except Exception as e:
            print(f"Failed to start Kafka application: {str(e)}")
            raise e

    def disconnect(self):
        # Implementation for disconnecting from Kafka
        print("Disconnecting from Kafka...")
        try:
            # Add disconnection logic here
            if hasattr(self, "app") and self.app is not None:
                self.app.stop()
            self.app = None
            self.dataframes.clear()
            #self.sdf_stream = None
            print("Disconnected from Kafka.")
        except Exception as e:
            print(f"Failed to disconnect from Kafka: {str(e)}")
            raise e

    def is_connected(self):
        # Check if the connection is active
        return (
            hasattr(self, "app")
            and self.app is not None
            and self.dataframes is not None
        )

    def get_connection_info(self) -> Dict[str, Any]:
        # Return connection information
        return {
            "address": self.address,
            "port": self.port,
            "consumer_group": self.consumer_group,
            "auto_offset_reset": self.auto_offset_reset,
            "security_protocol": self.security_protocol,
            "username": self.username,
        }

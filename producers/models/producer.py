"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro, CachedSchemaRegistryClient
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "broker_url":"PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094",
            "schema_registry_url":"http://localhost:8081"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            {"bootstrap.servers": self.broker_properties.get("broker_url")},
            schema_registry=CachedSchemaRegistryClient(self.broker_properties.get("schema_registry_url"))
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        try: 
            topic_list = []
            NewTopic(name=f"{self.topic_name}", num_partitions=self.num_partitions, replication_factor=self.num_replicas)
            AdminClient.create_topics(new_topics=topic_list)
            logger.info(f"topic creation kafka integration complete - topic: {self.topic_name} created")
        except:
            logger.info("topic creation kafka integration incomplete - skipping")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        try:
            self.producer.flush(10)
            logger.info("producer close begin")
        except:
            logger.info("producer close incomplete - skipping")
        finally:
            logger.info(f"producer close ended with {len(self.producer)} messages still in queue")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

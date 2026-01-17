"""
Kafka Producer and Event Logger Module
Handles all Kafka interactions for logging events
"""

import json
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError


class KafkaEventLogger:
    """Singleton Kafka producer for logging events"""

    _instance = None
    _producer = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(KafkaEventLogger, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize Kafka producer if not already initialized"""
        if self._producer is None:
            self._initialize_producer()

    def _initialize_producer(self):
        """Initialize Kafka producer with Aiven SSL configuration"""
        try:
            bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
            if not bootstrap_servers:
                raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable not set")

            # Write secrets from environment variables to temp files
            # This allows us to use secrets as env vars instead of mounted volumes
            ca_cert_content = os.environ.get('KAFKA_CA_CERT_CONTENT')
            cert_content = os.environ.get('KAFKA_CERT_CONTENT')
            key_content = os.environ.get('KAFKA_KEY_CONTENT')

            ca_file = '/tmp/ca.pem'
            cert_file = '/tmp/service.cert'
            key_file = '/tmp/service.key'

            if ca_cert_content:
                with open(ca_file, 'w') as f:
                    f.write(ca_cert_content)
                print("Written CA certificate to /tmp/ca.pem")

            if cert_content:
                with open(cert_file, 'w') as f:
                    f.write(cert_content)
                print("Written service certificate to /tmp/service.cert")

            if key_content:
                with open(key_file, 'w') as f:
                    f.write(key_content)
                print("Written service key to /tmp/service.key")

            self._producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(','),
                security_protocol="SSL",
                ssl_cafile=ca_file,
                ssl_certfile=cert_file,
                ssl_keyfile=key_file,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                # Producer configuration for reliability
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                max_in_flight_requests_per_connection=1,
                # Timeout settings
                request_timeout_ms=30000,
                # Batch settings for better performance
                linger_ms=10,
                batch_size=16384
            )

            # Log initialization
            self.log_event('kafka_producer_initialized', {
                'status': 'success',
                'bootstrap_servers': bootstrap_servers
            })

            print(f"Kafka producer initialized: {bootstrap_servers}")

        except Exception as e:
            print(f"Failed to initialize Kafka producer: {str(e)}")
            raise

    def log_event(self, event_type, event_data):
        """
        Log event to Kafka topic

        Args:
            event_type: Type/category of the event
            event_data: Dictionary containing event data
        """
        if self._producer is None:
            print("Kafka producer not initialized, skipping event logging")
            return

        try:
            kafka_topic = os.environ.get('KAFKA_TOPIC', 'csv-parquet-events')

            event = {
                'event_type': event_type,
                'timestamp': datetime.utcnow().isoformat(),
                'data': event_data
            }

            future = self._producer.send(
                kafka_topic,
                key=event_type,
                value=event
            )

            # Non-blocking: add callbacks
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)

        except Exception as e:
            print(f"Error logging event to Kafka: {event_type} - {str(e)}")

    def _on_send_success(self, metadata):
        """Callback for successful send"""
        print(f"Event sent to Kafka: {metadata.topic}:{metadata.partition}:{metadata.offset}")

    def _on_send_error(self, exception):
        """Callback for send error"""
        print(f"Failed to send event to Kafka: {exception}")

    def flush(self, timeout=5):
        """
        Flush all pending messages

        Args:
            timeout: Timeout in seconds
        """
        if self._producer:
            try:
                self._producer.flush(timeout=timeout)
            except Exception as e:
                print(f"Error flushing Kafka producer: {str(e)}")

    def close(self):
        """Close the Kafka producer"""
        if self._producer:
            try:
                self._producer.close(timeout=10)
                self._producer = None
                print("Kafka producer closed")
            except Exception as e:
                print(f"Error closing Kafka producer: {str(e)}")


# Global instance
_kafka_logger = None


def get_kafka_logger():
    """Get or create the global Kafka logger instance"""
    global _kafka_logger
    if _kafka_logger is None:
        _kafka_logger = KafkaEventLogger()
    return _kafka_logger


def log_event(event_type, event_data):
    """
    Convenience function to log an event

    Args:
        event_type: Type/category of the event
        event_data: Dictionary containing event data
    """
    try:
        logger = get_kafka_logger()
        logger.log_event(event_type, event_data)
    except Exception as e:
        print(f"Failed to log event: {event_type} - {str(e)}")

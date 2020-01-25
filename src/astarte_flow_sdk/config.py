import json


class ConfigLoader:
    def __init__(self):
        super().__init__()


class BlockConfig:
    def __init__(self, path):
        try:
            with open(path) as f:
                self.__data = json.load(f)
        except:
            pass

    def get_block_config(self):
        return self.__data


class RabbitMQConfig:
    def __init__(self, path):
        try:
            with open(path) as f:
                self.__data = json.load(f)
        except:
            raise

        try:
            self.__rabbitmq_config = self.__data["rabbitmq"]
        except:
            raise

        # Parse RabbitMQ configuration
        try:
            self.__rabbitmq_use_ssl = self.__rabbitmq_config.get("ssl", False)
            self.__rabbitmq_host = self.__rabbitmq_config["host"]
            self.__rabbitmq_port = self.__rabbitmq_config.get(
                "port", 5672 if not self.__rabbitmq_use_ssl else 5671)
            self.__rabbitmq_username = self.__rabbitmq_config["username"]
            self.__rabbitmq_password = self.__rabbitmq_config["password"]

            self.__rabbitmq_queues = self.__data.get("queues", [])
            self.__rabbitmq_exchange = self.__data.get("exchange", None)
        except:
            raise

    def get_rabbitmq_url(self):
        return f"{'amqps' if self.__rabbitmq_use_ssl else 'amqp'}://{self.__rabbitmq_username}:{self.__rabbitmq_password}@{self.__rabbitmq_host}:{self.__rabbitmq_port}/"

    def get_rabbitmq_consumer_queues(self):
        return self.__rabbitmq_queues

    def get_rabbitmq_producer_exchange(self):
        return self.__rabbitmq_exchange

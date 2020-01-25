import json
from datetime import datetime


class ValidationError(Exception):
    """The message could not be validated"""
    pass


class Message:
    def __init__(self,
                 key: str,
                 data,
                 payloadType: str,
                 timestamp: datetime = datetime.utcnow(),
                 metadata: dict = {}):
        self.__key = key
        self.__data = data
        self.__type = payloadType
        self.__timestamp = timestamp
        self.__metadata = metadata

        if not self.__type:
            if isinstance(self.__data, str):
                self.__type = "string"
            elif isinstance(self.__data, int):
                self.__type = "integer"
            elif isinstance(self.__data, float):
                self.__type = "real"
            elif isinstance(self.__data, datetime):
                self.__type = "datetime"
            elif isinstance(self.__data, bool):
                self.__type = "boolean"

    @classmethod
    def frompayload(cls, payload):
        j = json.loads(payload)
        try:
            return cls(j["key"], j["data"], j["type"],
                       datetime.utcfromtimestamp(j["timestamp"] / 1000),
                       j["metadata"])
        except Exception as exc:
            raise ValidationError from exc

    def marshal_flow_payload(self) -> str:
        payload = {
            "schema": "astarte_flow/message/v0.1",
            "key": self.__key,
            "data": self.__data,
            "type": self.__type,
            "timestamp": int(self.__timestamp.timestamp() * 1000),
            "timestamp_us": 0,
            "metadata": self.__metadata,
        }

        return json.dumps(payload)

    def generate_direct_followup_message(
        self,
        data,
        payloadType: str = '',
        timestamp: datetime = datetime.utcnow(),
        metadata: dict = {}):
        return Message(self.__key, data, payloadType, timestamp, metadata)

    def get_key(self):
        return self.__key

    def get_data(self):
        return self.__data

    def get_timestamp(self):
        return self.__timestamp

    def get_type(self):
        return self.__type

    def get_metadata(self):
        return self.__metadata

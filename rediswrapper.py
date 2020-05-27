import redis
from typing import NamedTuple, Type, Any, Dict, Optional, List


class RedisItem(NamedTuple):
    key: str
    default: Any
    type: Type


class RedisWrapper:
    def __init__(
        self, host: str = "localhost", port: int = 6379, namespace: Optional[str] = None
    ) -> None:
        self._client = redis.Redis(host, port)
        self._pubsub = self._client.pubsub()  # create publish/subscribe object
        self.namespace = namespace
        self.items: Dict[str, RedisItem] = {}

    def _namespace_key(self, key: str) -> str:
        if self.namespace is None:
            return key
        else:
            return self.namespace + ":" + key

    def _get(self, item: RedisItem) -> bytes:
        bytes_val = self._client.get(self._namespace_key(item.key))
        if item.type is None:
            return bytes_val
        elif item.type is bool:
            return True if bytes_val == b"true" else False
        else:
            return item.type(bytes_val)

    def _set(self, item: RedisItem, value: str) -> bool:
        # Use strings for bool values
        if isinstance(value, bool):
            val = b"true" if value else b"false"
        else:
            val = value

        return self._client.set(self._namespace_key(item.key), val)

    def add_item(self, item: RedisItem) -> None:
        """Add item to redis wrapper."""
        if item.key in self.items:
            raise KeyError(f"item with key {item.key} already exists")

        self.items[item.key] = item
        if self._get(item) is None:
            self._set(item, item.default)

    def add_items(self, items: List[RedisItem]) -> None:
        """Add a list of items to redis wrapper."""
        for item in items:
            self.add_item(item)

    def apply_defaults(self) -> None:
        """Reset all items to default values."""
        for _, item in self.items.items():
            self._set(item, item.default)

    def publish(self, channel: str, message: Any) -> int:
        """Publish message on channel. Returns the number of subscribers the message was delivered to."""
        self._client.publish(channel, message)

    def subscribe(self, *channels: str) -> None:
        """Subscribe to channels."""
        return self._pubsub.subscribe(*channels)

    def __getitem__(self, key: str) -> Any:
        item = self.items[key]
        return self._get(item)

    def __setitem__(self, key: str, value: Any) -> None:
        item = self.items[key]
        self._set(item, value)

    @property
    def keys(self) -> List[str]:
        return list(self.items.keys())
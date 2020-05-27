import time

import pytest
from rediswrapper import RedisWrapper, RedisItem


@pytest.fixture
def client():
    return RedisWrapper(namespace="rediswrapper")


def test_keys(client):
    client.add_item(RedisItem("dilation", 4, int))
    client.add_item(RedisItem("show_threshold", True, bool))
    client.apply_defaults()

    # default value
    val = client["dilation"]
    assert val == 4
    assert type(val) is int

    # set value with unknown key
    with pytest.raises(KeyError):
        client["foo"] = "bar"

    # set int value
    client["dilation"] = 2
    assert client["dilation"] == 2

    # set bool value
    client["show_threshold"] = True
    assert client["show_threshold"] is True

    client["show_threshold"] = False
    assert client["show_threshold"] is False


def test_publish_subscribe(client):
    client.subscribe("foo")

    time.sleep(0.1)
    msg = client.get_message()
    assert msg["channel"] == b"foo"
    assert msg["type"] == "subscribe"
    assert msg["data"] == 1

    client.publish("foo", "bar")
    time.sleep(0.1)
    msg = client.get_message()
    assert msg["channel"] == b"foo"
    assert msg["type"] == "message"
    assert msg["data"] == b"bar"

import pytest
from rediswrapper import RedisWrapper, RedisItem


def test_redis_wrapper():
    client = RedisWrapper(namespace="birdcam")
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

# -*- coding: utf-8 -*-

import pytest
from astarte_flow_sdk.skeleton import fib

__author__ = "Dario Freddi"
__copyright__ = "Dario Freddi"
__license__ = "mit"


def test_fib():
    assert fib(1) == 1
    assert fib(2) == 1
    assert fib(7) == 13
    with pytest.raises(AssertionError):
        fib(-10)

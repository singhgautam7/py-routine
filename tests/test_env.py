"""Tests for interpreter detection and the GIL warning."""

import importlib
import sys
import warnings

import pyroutine


def test_free_threading_matches_interpreter():
    checker = getattr(sys, "_is_gil_enabled", None)
    expected = (not checker()) if checker is not None else False
    assert pyroutine.free_threading() is expected


def test_gil_warning_fires_exactly_when_gil_is_enabled():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        importlib.reload(pyroutine)
    hits = [w for w in caught if issubclass(w.category, pyroutine.GILEnabledWarning)]
    if pyroutine.free_threading():
        assert not hits
    else:
        assert len(hits) == 1
        assert "pyroutine:" in str(hits[0].message)


def test_new_names_are_exported():
    assert "free_threading" in pyroutine.__all__
    assert "GILEnabledWarning" in pyroutine.__all__
    assert issubclass(pyroutine.GILEnabledWarning, RuntimeWarning)

"""Relay transport deadlines grow independently of API query/rate parameters."""

import vox_adaptive_relay
import vox_relay


def test_relay_transport_timeouts_are_tripled():
    for module in (vox_relay, vox_adaptive_relay):
        assert module.DEFAULTS["request_deadline"] == 2100
        assert module.DEFAULTS["socket_idle"] == 1890
        assert module.DEFAULTS["client_timeout"] == 30
    assert vox_relay.DEFAULTS["queue_wait"] == 360
    assert vox_adaptive_relay.DEFAULTS["queue_wait"] is None


def test_timeout_change_does_not_change_api_query_or_backoff_policy():
    assert vox_relay.INTEGER_BOUNDS["timeout"] == (10, 600)
    assert vox_relay.DEFAULTS["cooldown"] == 60
    assert vox_relay.DEFAULTS["backoff_cap"] == 900

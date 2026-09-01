from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from producer.job.model import ScheduleJobReq


def valid_request(**overrides):
    values = {
        "recipient": "person@example.com",
        "channel": "email",
        "payload": {"subject": "Hello"},
        "scheduled_for": datetime(2026, 9, 2, 10, tzinfo=timezone.utc),
        "priority": 2,
    }
    values.update(overrides)
    return values


def test_recipient_is_trimmed_but_otherwise_opaque():
    request = ScheduleJobReq.model_validate(
        valid_request(recipient="  +2348000000000  ", channel="sms")
    )

    assert request.recipient == "+2348000000000"


@pytest.mark.parametrize("recipient", ["", "   ", "x" * 513])
def test_recipient_must_be_non_empty_and_bounded(recipient):
    with pytest.raises(ValidationError):
        ScheduleJobReq.model_validate(valid_request(recipient=recipient))


def test_scheduled_time_must_be_timezone_aware():
    with pytest.raises(ValidationError, match="timezone-aware"):
        ScheduleJobReq.model_validate(
            valid_request(scheduled_for=datetime(2026, 9, 2, 10))
        )

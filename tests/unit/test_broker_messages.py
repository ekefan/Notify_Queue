from uuid import uuid4

from broker.messages import JobMessage


def test_job_message_round_trip():
    original = JobMessage(job_id=uuid4(), outbox_id=uuid4())

    decoded = JobMessage.decode(original.encode())

    assert decoded == original

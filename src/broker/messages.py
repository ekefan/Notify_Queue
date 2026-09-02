import json
from dataclasses import asdict, dataclass
from uuid import UUID


@dataclass(frozen=True)
class JobMessage:
    job_id: UUID
    outbox_id: UUID

    def encode(self) -> bytes:
        return json.dumps(
            {key: str(value) for key, value in asdict(self).items()}
        ).encode()

    @classmethod
    def decode(cls, body: bytes) -> "JobMessage":
        value = json.loads(body)
        return cls(job_id=UUID(value["job_id"]), outbox_id=UUID(value["outbox_id"]))

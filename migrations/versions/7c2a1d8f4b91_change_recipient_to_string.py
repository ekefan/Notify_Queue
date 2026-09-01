"""change recipient to string

Revision ID: 7c2a1d8f4b91
Revises: b45253b4902f
Create Date: 2026-09-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "7c2a1d8f4b91"
down_revision: Union[str, Sequence[str], None] = "b45253b4902f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "jobs",
        "recipient",
        existing_type=postgresql.UUID(),
        type_=sa.String(length=512),
        existing_nullable=False,
        postgresql_using="recipient::text",
    )


def downgrade() -> None:
    # This succeeds only while all stored destinations are UUID-compatible.
    op.alter_column(
        "jobs",
        "recipient",
        existing_type=sa.String(length=512),
        type_=postgresql.UUID(),
        existing_nullable=False,
        postgresql_using="recipient::uuid",
    )

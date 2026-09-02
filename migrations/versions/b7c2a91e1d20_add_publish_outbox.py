"""add durable broker publish outbox

Revision ID: b7c2a91e1d20
Revises: 0e605b57d5f3
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op


revision: str = "b7c2a91e1d20"
down_revision: str | Sequence[str] | None = "0e605b57d5f3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "publish_outbox",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("job_id", sa.UUID(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_publish_outbox_due",
        "publish_outbox",
        ["available_at", "created_at"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_publish_outbox_due", table_name="publish_outbox")
    op.drop_table("publish_outbox")

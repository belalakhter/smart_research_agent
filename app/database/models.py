from sqlalchemy import Column, Enum, LargeBinary, String
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.ext.declarative import declarative_base
import enum
import uuid

Base = declarative_base()


class StatusEnum(enum.Enum):
    pending = "pending"
    completed = "completed"


class Chat(Base):
    __tablename__ = "chat"

    id       = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name     = Column(String(255), nullable=False, default="New Conversation")
    messages = Column(JSONB, nullable=False, default=list)

    def __repr__(self):
        return f"<Chat(id={self.id}, name={self.name!r})>"


class Document(Base):
    __tablename__ = "document"

    id       = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    filename = Column(String(255), nullable=False, default="untitled")
    content  = Column(LargeBinary, nullable=False)
    status   = Column(Enum(StatusEnum), default=StatusEnum.pending, nullable=False)

    def __repr__(self):
        return f"<Document(id={self.id}, filename={self.filename!r}, status={self.status})>"
import datetime
import enum
import uuid

from sqlalchemy.dialects.postgresql import UUID, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Boolean, Integer, String, DateTime, ForeignKey, Text, ForeignKey, Enum
from sqlalchemy.exc import IntegrityError, PendingRollbackError
from psycopg2.errors import UniqueViolation


Base = declarative_base()


class User(Base):
    __tablename__ = 'users'

    id = Column('id', UUID(as_uuid=True), primary_key=True, index=True)
    message_id = Column(UUID(as_uuid=True), ForeignKey('messages.id'), index=True)

    def __repr__(self):
        return "id='{id}',message_id='{message_id}'>".format(
            id=self.id,
            message_id=self.message_id,
        )

class Channel(Base):
    __tablename__ = 'channels'

    id = Column('id', UUID(as_uuid=True), primary_key=True, index=True)
    message_id = Column(UUID(as_uuid=True), ForeignKey('messages.id'), index=True)

    def __repr__(self):
        return "id='{id}',message_id='{message_id}'>".format(
            id=self.id,
            message_id=self.message_id,
        )

class Correlation(Base):
    __tablename__ = 'correlations'

    id = Column('id', UUID(as_uuid=True), primary_key=True, index=True)
    message_id = Column(UUID(as_uuid=True), ForeignKey('messages.id'), index=True)

    def __repr__(self):
        return "id='{id}',message_id='{message_id}'>".format(
            id=self.id,
            message_id=self.message_id,
        )

class MessageContent(Base):
    __tablename__ = 'messages_content'

    id = Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    body = Column('body', Text, nullable=False, default='')
    message_id = Column(UUID(as_uuid=True), ForeignKey('messages.id'), index=True)

    def __repr__(self):
        return "id='{id},message_id='{message_id}',body='{body}'>".format(
            id=self.id,
            message_id=self.message_id,
            body=self.body
        )

class MessageState(enum.Enum):
    new = 1
    read = 2
    sent = 3
    failed = 4

class MessageType(enum.Enum):
    message = 1
    trainer_alert = 2
    placeholder = 3
    activity_summary = 4
    activity_summary_updated = 5

# I did not find an example with populated sub_type,
# but did not want to waste space storing as string instead of int.
class MessageSubType(enum.Enum):
    example_sub_type = 1

class AssociatedType(enum.Enum):
    user = 1
    goal = 2
    workout = 3
    activity_summary = 4

class DeliveryType(enum.Enum):
    sms = 1
    push = 2

class Message(Base):
    __tablename__ = 'messages'

    # Used to temp store full json message.
    json_message = {}

    id = Column('id', UUID(as_uuid=True), primary_key=True)
    created_at = Column('created_at', DateTime, nullable=False, default=datetime.datetime.now(), index=True)
    updated_at = Column('updated_at', DateTime, nullable=True)
    send_at = Column('send_at', DateTime, nullable=False)
    sent_at = Column('sent_at', DateTime, nullable=False)

    from_user_id = Column(UUID(as_uuid=True), ForeignKey(User.id), index=True, nullable=False)
    from_user = relationship("User", foreign_keys=[from_user_id])
    to_user_id = Column(UUID(as_uuid=True), ForeignKey(User.id), index=True, nullable=False)
    to_user = relationship("User", foreign_keys=[to_user_id])

    body_id = Column(UUID(as_uuid=True), ForeignKey(MessageContent.id), nullable=True)
    body = relationship("MessageContent", foreign_keys=[body_id])

    state = Column(Enum(MessageState), default=1, index=True)
    read_at = Column('read_at', DateTime, nullable=True)
    sent_automatically = Column('sent_automatically', Boolean, nullable=False, default=False)
    tag = Column('tag', String(255), nullable=False, default='')
    _type = Column(Enum(MessageType), default=1, index=True, nullable=False)

    # TODO: Move to messages_associations?? Not sure what associations is.
    associated_type = Column(Enum(AssociatedType), nullable=True)
    associated_id = Column('associated_id', UUID(as_uuid=True), nullable=True)

    is_flagged = Column('is_flagged', Boolean, nullable=False, default=False)
    # TODO: fix parse
    # year 1539535387 is out of range
    # tsParser = dateutil.parser.parser()
    # message.slack_ts = tsParser.parse(json_message['slack_ts'])
    # slack_ts = Column('slack_ts', DateTime, nullable=True)
    slack_ts = Column('slack_ts', String(255), nullable=True)
    channel_id = Column(UUID(as_uuid=True), ForeignKey(Channel.id), nullable=True, index=True)
    channel = relationship("Channel", foreign_keys=[channel_id])
    canceled_at = Column('canceled_at', DateTime, nullable=True)
    deleted_at = Column('deleted_at', DateTime, nullable=True)

    # This could also be a jsonb column with indexed known keys or moved to a different table.
    attributes = Column('attributes', JSON, nullable=True)

    acted_on_at = Column('acted_on_at', DateTime, nullable=True)
    sender_user_id = Column(UUID(as_uuid=True), ForeignKey(User.id), nullable=True, index=True)
    sender_user = relationship("User", foreign_keys=[sender_user_id])

    correlation_id = Column(UUID(as_uuid=True), ForeignKey(Correlation.id), nullable=True, index=True)
    correlation = relationship("Correlation", foreign_keys=[correlation_id])

    # I did not find an example with populated sub_type,
    # but did not want to waste space storing as string instead of int.
    sub_type = Column(Enum(MessageSubType), nullable=True)

    viewed_at = Column('viewed_at', DateTime, nullable=True)
    viewed_duration = Column('viewed_duration', Integer, nullable=False, default=0)

    # Did not find an example, but may use json here instead.
    urls = Column('urls', String(255), nullable=False, default='')

    duration = Column('duration', Integer, nullable=False, default=0)
    paused_at = Column('paused_at', DateTime, nullable=True)
    delivery_type = Column(Enum(DeliveryType), nullable=True, index=True)
    notification_count = Column('notification_count', Integer, nullable=False, default=0)

    def __repr__(self):
        return "id='{id}',message='{message_type}'>".format(
            id=self.id,
            message_type=self._type
        )

# Used for getting / creating associated tables.
# user = get_or_create(session, User, id=id)
def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        session.add(instance)
        try:
          session.commit()
        # Due to threading, it's possible another thread created the table before we could create it.
        # In that case, query again and send instance.
        except IntegrityError as e:
            if isinstance(e.orig, UniqueViolation): 
              session.rollback()
              instance = session.query(model).filter_by(**kwargs).first()
              return instance
            else:
                raise e
        except PendingRollbackError as e:
            session.rollback()
            instance = session.query(model).filter_by(**kwargs).first()
            return instance
        return instance

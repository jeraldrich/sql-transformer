import json
import logging
from datetime import datetime

from consumers.models import Message, User, Channel, Correlation, MessageContent, MessageState, MessageType, get_or_create


logger = logging.getLogger('message_parser')

class JsonMessageParser():

    def parse(self, json_message):
        """
        parse each json message and match against table to insert into based off patterns
        """
        message = None

        if json_message['type'] in [message_type.name for message_type in MessageType]:
            message = Message(
                id=json_message['id'],
                created_at=json_message['created_at'],
                updated_at=json_message['updated_at'],
                send_at=json_message['send_at'],
                sent_at=json_message['sent_at'],
                read_at=json_message['read_at'],
                sent_automatically=json_message['sent_automatically'],
                tag=json_message['tag'],
                associated_id=json_message['associated_id'],
                is_flagged=json_message['is_flagged'],
                canceled_at=json_message['canceled_at'],
                deleted_at=json_message['deleted_at'],
                attributes=json_message['attributes'],
                acted_on_at=json_message['acted_on_at'],
                viewed_at=json_message['viewed_at'],
                viewed_duration=json_message['viewed_duration'],
                urls=json_message['urls'],
                duration=json_message['duration'],
                paused_at=json_message['paused_at'],
                notification_count=json_message['notification_count'],
            )
            message.json_message = json_message
            # Set enums. TODO: Move this to init to auto clean / set blank values.
            if json_message['type']:
              message._type = json_message['type']
            if json_message['state']:
              message.state = json_message['state']
            if json_message['associated_type']:
              message.associated_type = json_message['associated_type']
            if json_message['sub_type']:
              message.sub_type = json_message['sub_type']
            if json_message['delivery_type']:
              message.delivery_type = json_message['delivery_type']
            # Set timestamps.
            if json_message['updated_at']:
              message.updated_at = json_message['updated_at']
            if json_message['send_at']:
              message.send_at = json_message['send_at']
            if json_message['sent_at']:
              message.sent_at = json_message['sent_at']
            if json_message['read_at']:
              message.read_at = json_message['read_at']
            if json_message['slack_ts']:
                # TODO: fix parse
                # year 1539535387 is out of range
                # tsParser = dateutil.parser.parser()
                # message.slack_ts = tsParser.parse(json_message['slack_ts'])
                message.slack_ts = json_message['slack_ts']
            if json_message['canceled_at']:
              message.canceled_at = json_message['canceled_at']
            if json_message['deleted_at']:
              message.deleted_at = json_message['deleted_at']
            if json_message['acted_on_at']:
              message.acted_on_at = json_message['acted_on_at']
            if json_message['viewed_at']:
              message.viewed_at = json_message['viewed_at']
            if json_message['paused_at']:
              message.paused_at = json_message['paused_at']
            if json_message['associated_id']:
              message.associated_id = json_message['associated_id']
        else:
            logger.error('Invalid message type [{message_type}] detected'.format(message_type=json_message['type']))
            return None

        return message
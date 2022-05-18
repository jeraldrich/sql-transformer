from multiprocessing import Process, cpu_count, Manager
from os import sys
import time
import logging
import urllib.request, json
from queue import Empty

from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import asc

from producers import JsonMessageParser
from consumers import create_pg_pool
from consumers.models import Message, User, Channel, Correlation, MessageContent, MessageState, MessageType, get_or_create
from settings import JSON_URLS


logger = logging.getLogger('message_parser')
logger.setLevel(logging.DEBUG)
logging.basicConfig()
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
logger.addHandler(stream_handler)


def producer_queue(queue, parser):
    for json_url in JSON_URLS:
        with urllib.request.urlopen(json_url) as jsonResponse:
            data = json.loads(jsonResponse.read().decode())
            for json_message in data:
                parsed_data = parser.parse(json_message)
                queue.put(parsed_data)
    queue.put('ProducerFinished')


def consumer_queue(proc_id, queue):

    # shared pooled session per consumer proc
    pg_pool = create_pg_pool()
    session_factory = sessionmaker(pg_pool)
    Session = scoped_session(session_factory)

    while True:
        try:
            time.sleep(0.1)
            message = queue.get(proc_id, 1)
            if message == 'ProducerFinished' and queue.qsize() == 0:
                logger.info('ProducerFinished received')
                # put stop back in queue for other consumers
                queue.put('ProducerFinished')
                break
            logger.info(message)
            if message != None:
                session = Session()
                existing_message = session.query(Message).get(message.id)
                if not existing_message:
                    # Create / Set fk relations
                    message.from_user = get_or_create(session, User, id=message.json_message['from_user_id'])
                    message.to_user = get_or_create(session, User, id=message.json_message['to_user_id'])
                    if message.json_message['channel_id']:
                      message.channel = get_or_create(session, Channel, id=message.json_message['channel_id'])
                    if message.json_message['correlation_id']:
                      message.correlation = get_or_create(session, Correlation, id=message.json_message['correlation_id'])
                    if message.json_message['sender_user_id']:
                      message.sender_user = get_or_create(session, User, id=message.json_message['sender_user_id'])
                    session.add(message)
                    session.commit()
                    # After message creation, associate message content to store message body.
                    if message.json_message['body']:
                        message.body = get_or_create(session, MessageContent, message_id=message.id, body=message.json_message['body'])
                        session.add(message)
                        session.commit()
        except Empty:
            pass


class ParserManager(object):

    def __init__(self):
        self.manager = Manager()
        self.queue = self.manager.Queue()
        self.NUMBER_OF_PROCESSES = cpu_count()
        self.parser = JsonMessageParser()

    def start(self):
        self.producer = Process(
            target=producer_queue,
            args=(self.queue, self.parser)
        )
        self.producer.start()

        self.consumers = [
            Process(target=consumer_queue, args=(i, self.queue))
            for i in range(self.NUMBER_OF_PROCESSES)
        ]
        for consumer in self.consumers:
            consumer.start()

    def join(self):
        self.producer.join()
        for consumer in self.consumers:
            consumer.join()

if __name__ == '__main__':
    try:
        manager = ParserManager()
        manager.start()
        manager.join()
    except (KeyboardInterrupt, SystemExit):
        logger.info('interrupt signal received')
        sys.exit(1)
    except Exception as e:
        raise e

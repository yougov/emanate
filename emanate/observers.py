"""Classes for observers in ``emanate``."""
import logging
import time
from threading import Thread

from six.moves import queue
from pika.exceptions import AMQPError, RecursionError
from pikachewie.data import Properties
from pikachewie.helpers import broker_from_config
from pikachewie.publisher import BlockingJSONPublisher

log = logging.getLogger(__name__)


class BasePublisher(object):
    """Base Publisher class."""
    def __init__(self, *args, **kwargs):
        pass

    def publish(self, event):
        pass


NullPublisher = BasePublisher
"""Publisher that silently discards all events."""


class QueuePublisher(BasePublisher):
    """Publisher that dispatches events to a `queue.Queue`."""
    def __init__(self):
        self._queue = queue.Queue(maxsize=256)
        self._worker = Thread(target=self._process_queue)
        self._worker.daemon = True
        self._worker.start()

    def publish(self, event):
        try:
            self._queue.put_nowait(event)
        except queue.Full as exc:
            log.exception('Cannot publish event: %r', exc)
            log.warn('Failed to publish event %s with context %s',
                     event.data, event.context)

    def _process_queue(self):
        while True:
            event = self._queue.get()
            try:
                self._process_event(event)
            except Exception as exc:
                log.exception(exc)
            finally:
                self._queue.task_done()

    def _process_event(self, event):
        pass


class RabbitMQPublisher(QueuePublisher):
    """Publisher that publishes events as messages to RabbitMQ.

    By default, messages are published on a headers exchange. The name of the
    exchange is given in the `config` passed to the Publisher's constructor.

    If the 'incessant' flag is set, then the publisher will continuously attempt
    to try and publish an event regardless of any errors - no new events will be
    processed until the current event has been successfully published.
    """
    def __init__(self, config, broker_name='default', incessant=True):
        self.config = config
        self.broker_name = broker_name
        self.broker = broker_from_config(
            self.config['rabbitmq']['brokers'][self.broker_name]
        )
        self.publisher = BlockingJSONPublisher(self.broker)
        self.exchange = self.config['rabbitmq']['default_exchange']
        self.incessant = incessant
        self._initialize()
        super(RabbitMQPublisher, self).__init__()

    def _initialize(self):
        # declare the exchange (will trigger a connection to the broker)
        self.publisher.channel.exchange_declare(
            self.exchange,
            **self.config['rabbitmq']['exchanges'][self.exchange]
        )

    def _wait_for_event(self):
        while True:
            # trigger processing of RabbitMQ data events
            try:
                self.publisher.process_data_events()
            except Exception as exc:
                log.exception('Error processing data events: %r', exc)
                del self.publisher.channel

            try:
                return self._queue.get(timeout=10)
            except queue.Empty:
                pass

    def _process_queue(self):
        while True:
            event = self._wait_for_event()
            self._handle_event(event)

    def _handle_event(self, event):
        is_retry = False
        try:
            while True:
                published = self._process_event(event, is_retry=is_retry)
                if self.incessant and not published:
                    log.info('Will attempt to republish event.')
                    time.sleep(0.2) # Wait briefly before trying again.
                    is_retry = True
                    continue
                return
        except Exception as exc:
            log.exception(exc)
        finally:
            self._queue.task_done()

    def _process_event(self, event, is_retry=False):
        '''Returns true if the event was published, false otherwise.'''
        properties = Properties()
        if event.context:
            properties.headers = event.context

        try:
            self.publisher.publish(self.exchange, '', event.data, properties)
            return True
        except (AMQPError, RecursionError) as exc:
            # To stop us generate too many logs when repeatedly retrying to
            # publish an event, we only produce a full log on the initial
            # attempt.
            if not is_retry:
                log.exception('Cannot publish to RabbitMQ: %r', exc)
                log.warn('Failed to publish message payload %s with context %s',
                         event.data, event.context)
            return False

import ConfigParser
import json
import logging
import multiprocessing
import os
import sys
import time
import traceback
from datetime import datetime, timedelta

import redis
from kafka import KafkaConsumer


KAFKA_URL = None
KAFKA_TOPIC = None
KAFKA_GROUP_ID = None
VALID_EVENT_NAMES = None
PARTITIONS = None
REDIS_URL = None
REDIS_PORT = None
REDIS_TTL = None
REPORT_OFFSET = None

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def reportSession(batch, redisClient):
    p = redisClient.pipeline(transaction=False)
    for item in batch:
        sid = item[0]
        vid = item[1]
        p.setex(sid, REDIS_TTL, vid)
    p.execute()


def getTimestampByMinute(atime):
    epoch = datetime(1970, 1, 1)
    now = atime.replace(second=0, microsecond=0)
    return int((now - epoch).total_seconds())


def start(partitionId, pool):
    batch = []
    lastProcessTs = datetime.utcnow()
    startWindow = getTimestampByMinute(lastProcessTs - timedelta(minutes=REPORT_OFFSET))

    kafkaConsumer = getKafkaConsumer(partitionId, retry=True)
    redisClient = redis.Redis(connection_pool=pool)

    while True:
        messages, kafkaConsumer = fetchKafkaMessages(partitionId, kafkaConsumer)

        for m in messages:
            kafkaConsumer.task_done(m)

            item = getEventItemFromKafkaItem(m)
            item = filterEventItem(item, startWindow)

            if item:
                batch.append((item['sid'], item['vid']))

        elapsed = (datetime.utcnow() - lastProcessTs).total_seconds()
        batchSize = len(batch)
        if batchSize > 0 and (batchSize >= 50 or elapsed >= 1):
            reportSession(batch, redisClient)
            batch = []
            lastProcessTs = datetime.utcnow()


def getKafkaConsumer(partitionId, retry=False):
    consumer = None
    while not consumer:
        try:
            consumer = KafkaConsumer(bootstrap_servers=[KAFKA_URL],
                                     group_id=KAFKA_GROUP_ID,
                                     auto_commit_enable=True,
                                     auto_commit_interval_ms=30 * 1000,
                                     auto_offset_reset='largest')

            consumer.set_topic_partitions((KAFKA_TOPIC, partitionId))

            logger.info('connect to Kafka:[%s] topic:[%s] partition:[%s] success' % (KAFKA_URL, KAFKA_TOPIC, partitionId))
        except Exception as e:
            if retry:
                logger.warn("failed to connect to Kafka:[%s] topic:[%s] partition:[%s], retry in 1s" % (KAFKA_URL, KAFKA_TOPIC, partitionId))
                time.sleep(1)
            else:
                logger.error("failed to connect to Kafka:[%s] topic:[%s] partition:[%s]" % (KAFKA_URL, KAFKA_TOPIC, partitionId))
                raise e
    return consumer


def fetchKafkaMessages(partitionId, consumer):
    messages = None
    while not messages:
        try:
            messages = consumer.fetch_messages()
        except Exception as e:
            traceback.print_exc()
            logger.warn('failed to fetch Kafka messages from:[%s], error:%s' % (KAFKA_URL, e))
            consumer = getKafkaConsumer(partitionId, True)
            continue

    return messages, consumer


def filterEventItem(item, startWindow):
    if item is  None:
        return None

    if 'ts' not in item or int(item['ts'] / 1000) < startWindow:
        return None

    if 'name' not in item or item['name'] not in VALID_EVENT_NAMES:
        return None

    if 'sid' not in item or not item['sid']:
        return None

    return item


def getEventItemFromKafkaItem(kafkaItem):
    try:
        return json.loads(kafkaItem.value)
    except Exception, e:
        logger.warn('invalid event item:%s error:%s' % (kafkaItem.value, e))
        return None


def initConfig():
    configFile = os.path.dirname(os.path.realpath(__file__))+'/product.ini'

    if not os.path.isfile(configFile):
        logger.error('config not exist:%s' %(configFile))
        sys.exit(1)

    config = ConfigParser.RawConfigParser()
    config.read(configFile)

    global KAFKA_URL
    global KAFKA_TOPIC
    global KAFKA_GROUP_ID
    global VALID_EVENT_NAMES
    global PARTITIONS
    global REDIS_URL
    global REDIS_PORT
    global REDIS_TTL
    global REPORT_OFFSET

    KAFKA_URL = config.get('kafka', 'url')
    KAFKA_TOPIC = config.get('kafka', 'topic')
    KAFKA_GROUP_ID = config.get('kafka', 'group_id')
    VALID_EVENT_NAMES = set(config.get('kafka', 'event_names').split(","))
    PARTITIONS = [int(x) for x in config.get('kafka', 'partitions').split(',')]
    REDIS_URL = config.get('redis', 'url')
    REDIS_PORT = int(config.get('redis', 'port'))
    REDIS_TTL = int(config.get('redis', 'ttl'))
    REPORT_OFFSET = int(config.get('report', 'offset'))

    logger.info("init from config:%s successfully" % configFile)


def getRedisConnectionPool():
    return redis.ConnectionPool(host=REDIS_URL, port=REDIS_PORT, db=1)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s')

    logger.info("start session_data_collector")

    initConfig()
    pool = getRedisConnectionPool()
    jobs = []

    for i in PARTITIONS:
        p = multiprocessing.Process(target=start, args=(i,pool))
        p.daemon = True
        jobs.append(p)
        p.start()

    for j in jobs:
        j.join()

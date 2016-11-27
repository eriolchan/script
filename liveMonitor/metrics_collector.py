import ConfigParser
import logging
import multiprocessing
import os
import sys
import time
import traceback
from datetime import datetime, timedelta

import cjson
import redis

import potsdb
from kafka import KafkaConsumer

KAFKA_URL = None
KAFKA_TOPIC = None
KAFKA_GROUP_ID = None
VALID_EVENT_NAMES = None
PARTITIONS = None
REDIS_URL = None
REDIS_PORT = None
REDIS_TTL = None
REPORT_DELAY = None
REPORT_INTERVAL = None
REPORT_OFFSET = None
REPORT_SAMPLE = None
REPORT_BATCH = None
VALID_VIDS = None
OPENTSDB_URL = None
COUNTER_KEYS = None

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def start(partitionId, pool):
    batch = []
    lastProcessTs = datetime.utcnow()
    startWindow = getTimestampByMinute(lastProcessTs - timedelta(minutes=REPORT_OFFSET))

    kafkaConsumer = getKafkaConsumer(partitionId, retry=True)
    redisClient = redis.StrictRedis(connection_pool=pool)
    vidMapping = redis.StrictRedis(host=REDIS_URL, port=REDIS_PORT, db=1)

    while True:
        messages, kafkaConsumer = fetchKafkaMessages(partitionId, kafkaConsumer)

        for m in messages:
            kafkaConsumer.task_done(m)

            item = getEventItemFromKafkaItem(m)
            item = filterEventItem(item, startWindow)

            if item:
                batch.append((item['id'], int(item['ts'] / 1000), item['sid'], item['value']))

        elapsed = (datetime.utcnow() - lastProcessTs).total_seconds()
        batchSize = len(batch)
        if batchSize > 0 and (batchSize >= REPORT_BATCH or elapsed >= 20):
            processData(batch, vidMapping, redisClient)
            batch = []
            lastProcessTs = datetime.utcnow()
            #logger.info("pid:%d, batchSize:%d, elapsed:%d" % (partitionId, batchSize, elapsed))


def getTimestampByMinute(atime):
    epoch = datetime(1970, 1, 1)
    now = atime.replace(second=0, microsecond=0)
    return int((now - epoch).total_seconds())


def getKafkaConsumer(partitionId, retry=False):
    consumer = None
    while not consumer:
        try:
            consumer = KafkaConsumer(bootstrap_servers=[KAFKA_URL],
                                     group_id=KAFKA_GROUP_ID,
                                     auto_commit_enable=True,
                                     auto_commit_interval_ms=30* 1000,
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


def getEventItemFromKafkaItem(kafkaItem):
    try:
        return cjson.decode(kafkaItem.value)
    except Exception as e:
        logger.warn('invalid event item:%s error:%s' % (kafkaItem.value, e))
        return None


def filterEventItem(item, startWindow):
    if item is None:
        return None

    if 'ts' not in item or int(item['ts'] / 1000) < startWindow:
        return None

    if 'id' not in item or item['id'] not in VALID_EVENT_NAMES:
        return None

    if 'sid' not in item or not item['sid']:
        return None

    return item


def processData(batch, vidMapping, redisClient):
    sids = set()
    hosts = set()
    records = []

    for item in batch:
        id = item[0]
        ts = item[1]
        sid = item[2]
        value = item[3]

        sids.add(sid)
        if id == 86 and value == 640:
            hosts.add(sid)
        elif id == 140 or (id == 83 and sid in hosts):
            records.append((id, ts, sid, value))

    sidToVid = getSidToVid(list(sids), vidMapping)

    p = redisClient.pipeline(transaction=False)
    for item in records:
        id = item[0]
        ts = item[1] - item[1] % REPORT_SAMPLE
        sid = item[2]
        value = item[3]
        vid = sidToVid[sid]

        if vid and vid in VALID_VIDS:
            tsKey = '%d:%s' % (ts, vid)
            p.zadd('TS', ts, tsKey)

            if id == 140:
                p.hincrby(tsKey, 'total', 1)
                if value:
                    p.hincrby(tsKey, 'freeze', 1)
            elif value:
                score = getScore(value)
                if score:
                    p.hincrby(tsKey, score, 1)

    p.execute()


def getSidToVid(sids, vidMapping):
    vp = vidMapping.pipeline(transaction=False)
    for sid in sids:
        vp.get(sid)
    vids = vp.execute()
    return dict(zip(sids, vids))


def getScore(value):
    if value > 600:
        return 'b5'
    elif value > 500:
        return 'b4'
    elif value > 400:
        return 'b3'
    elif value > 300:
        return 'b2'
    else:
        return 'b1'


def initConfig():
    configFile = os.path.dirname(os.path.realpath(__file__)) + '/product.ini'

    if not os.path.isfile(configFile):
        logger.error('config not exist:%s' % configFile)
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
    global REPORT_DELAY
    global REPORT_INTERVAL
    global REPORT_OFFSET
    global REPORT_SAMPLE
    global REPORT_BATCH
    global VALID_VIDS
    global OPENTSDB_URL
    global COUNTER_KEYS

    KAFKA_URL = config.get('kafka', 'url')
    KAFKA_TOPIC = config.get('kafka', 'topic')
    KAFKA_GROUP_ID = config.get('kafka', 'group_id')
    VALID_EVENT_NAMES = set(int(x) for x in config.get('kafka', 'event_names').split(','))
    PARTITIONS = [int(x) for x in config.get('kafka', 'partitions').split(',')]

    REDIS_URL = config.get('redis', 'url')
    REDIS_PORT = int(config.get('redis', 'port'))
    REDIS_TTL = int(config.get('redis', 'ttl'))

    REPORT_DELAY = int(config.get('report', 'delay'))
    REPORT_INTERVAL = int(config.get('report', 'interval'))
    REPORT_OFFSET = int(config.get('report', 'offset'))
    REPORT_SAMPLE = int(config.get('report', 'sample'))
    REPORT_BATCH = int(config.get('report', 'batch'))
    VALID_VIDS = set(config.get('report', 'vids').split(','))

    OPENTSDB_URL = config.get('opentsdb', 'url')

    COUNTER_KEYS = {'total': 0, 'freeze': 1, 'b5': 2, 'b4': 3, 'b3': 4, 'b2': 5, 'b1': 6}

    logger.info("init from config:%s successfully" % configFile)


def reportMetrics(pool):
    p = redis.StrictRedis(connection_pool=pool).pipeline(transaction=False)
    opentsdbClient = potsdb.Client(OPENTSDB_URL)

    while True:
        time.sleep(REPORT_INTERVAL * 60)

        lastWindow = getTimestampByMinute(datetime.utcnow() - timedelta(minutes=REPORT_DELAY))
        p.zrangebyscore('TS', 0, lastWindow)
        p.zremrangebyscore('TS', 0, lastWindow)
        tsRange = p.execute()

        metricsCounter = {}
        for tsKey in tsRange[0]:
            p.hgetall(tsKey)
        results = p.execute()

        for tsKey, result in zip(tsRange[0], results):
            metricsCounter[tsKey] = [0, 0, 0, 0, 0, 0, 0]
            for key, value in result.iteritems():
                index = COUNTER_KEYS[key]
                metricsCounter[tsKey][index] = int(value)
                p.hdel(tsKey, key)

        p.execute()

        reportOpenTsdb(metricsCounter, opentsdbClient)


def reportOpenTsdb(metricsCounter, opentsdbClient):
    freezerateMetric = 'monitor.video.freezerate'
    bitrateMetric = 'monitor.video.bitrate'

    for tsKey, value in metricsCounter.iteritems():
        ts, vid = parseTsKey(tsKey)

        freezeRate = 1.0 * value[1] / value[0] if value[0] else 0
        opentsdbClient.log(freezerateMetric, freezeRate, timestamp=ts, vendor=vid)

        opentsdbClient.log(bitrateMetric, value[2], timestamp=ts, vendor=vid, score=5)
        opentsdbClient.log(bitrateMetric, value[3], timestamp=ts, vendor=vid, score=4)
        opentsdbClient.log(bitrateMetric, value[4], timestamp=ts, vendor=vid, score=3)
        opentsdbClient.log(bitrateMetric, value[5], timestamp=ts, vendor=vid, score=2)
        opentsdbClient.log(bitrateMetric, value[6], timestamp=ts, vendor=vid, score=1)

        #logger.info("tsKey=%s, rate=%f, b5=%d, b4=%d, b3=%d, b2=%d, b1=%d" % (tsKey, freezeRate, value[2], value[3], value[4], value[5], value[6]))


def parseTsKey(tsKey):
    fields = tsKey.split(':')
    if len(fields) == 2:
        return int(fields[0]), fields[1]


def getRedisConnectionPool():
    return redis.ConnectionPool(host=REDIS_URL, port=REDIS_PORT, db=0)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s')

    logger.info("start metrics_collector")

    initConfig()
    pool = getRedisConnectionPool()
    jobs = []

    for i in PARTITIONS:
        p = multiprocessing.Process(target=start, args=(i, pool))
        p.daemon = True
        jobs.append(p)
        p.start()

    reportMetrics(pool)

    for j in jobs:
        j.join()

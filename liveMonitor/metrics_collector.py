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
COUNTER_NAMES = None

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
                                     auto_commit_interval_ms=30 * 1000,
                                     auto_offset_reset='largest')

            consumer.set_topic_partitions((KAFKA_TOPIC, partitionId))

            logger.info("connect to Kafka:[%s] topic:[%s] partition:[%s] success" % (KAFKA_URL, KAFKA_TOPIC, partitionId))
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
            logger.warn("failed to fetch Kafka messages from:[%s], error:%s" % (KAFKA_URL, e))
            consumer = getKafkaConsumer(partitionId, True)
            continue

    return messages, consumer


def getEventItemFromKafkaItem(kafkaItem):
    try:
        return cjson.decode(kafkaItem.value)
    except Exception as e:
        logger.warn("invalid event item:%s error:%s" % (kafkaItem.value, e))
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
    sids, hosts, guests, presents, records = getRoleSet(batch)
    sidToVid = getSidToVid(list(sids), vidMapping)

    count = [0, 0, 0]
    metrics = {}
    p = redisClient.pipeline(transaction=False)
    for item in records:
        id = item[0]
        lts = item[1]
        sid = item[2]
        value = item[3]

        vid = sidToVid[sid]
        if vid and vid in VALID_VIDS:
            ts = lts - lts % REPORT_SAMPLE
            tsKey = '%d:%s' % (ts, vid)
            p.zadd('TS', ts, tsKey)

            if tsKey not in metrics:
                metrics[tsKey] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

            if id == 140:
                totalIndex = 0 if sid in presents else 9
                metrics[tsKey][totalIndex] += 1
            
            index = getMetricIndex(id, sid, value, hosts, guests, presents)
            if index != -1:
                metrics[tsKey][index] += 1
                
                # report sid whose score = 1
                if vid == '15840' and index == 6 and count[0] < 3:
                    reportBadSids(lts, sid, vid, value, 'b1', p)
                    count[0] += 1
                if vid == '15840' and index == 8 and count[1] < 3:
                    reportBadSids(lts, sid, vid, value, 'g1', p)
                    count[1] += 1
                if vid == '6727' and index == 10 and count[2] < 5:
                    reportBadSids(lts, sid, vid, value, 'af', p)
                    count[2] += 1
    
    reportRedis(metrics, p)


def reportRedis(metrics, p):
    for key, value in metrics.iteritems():
        for i in range(len(value)):
            if value[i]:
                p.hincrby(key, COUNTER_NAMES[i], value[i])
    p.execute()


def getMetricIndex(id, sid, value, hosts, guests, presents):
    """
    get index of metrics

    0: total count of presents
    1: freeze count of presents
    2: b5 of hosts
    3: b4 of hosts
    4: b3 of hosts
    5: b2 of hosts
    6: b1 of hosts
    7: g2 of guests
    8: g1 of guests
    9: total count of audience
    10: freeze count of audience
    """

    result = -1
    if id == 140 and value:
        result = 1 if sid in presents else 10
    elif id == 83:
        if sid in hosts:
            result = getHostScore(value)
        elif sid in guests:
            result = getGuestScore(value)    
    return result


def reportBadSids(ts, sid, vid, value, score, p):
    badSids = '%s:%d:%s:%s:%d' % (score, ts, sid, vid, value)
    p.setex(badSids, 180, 1)


def getRoleSet(batch):
    sids = set()
    hosts = set()
    guests = set()
    presents = set()
    records = []

    for item in batch:
        id = item[0]
        ts = item[1]
        sid = item[2]
        value = item[3]

        sids.add(sid)
        if id == 86:  # Video/High Send Resolution/Height
            if value == 640:
                hosts.add(sid)
            elif value == 160:
                guests.add(sid)
        elif id == 140:  # Video Recv Render Freeze Count
            records.append((id, ts, sid, value))
        elif id == 83 and value:  # Video/High Send Bitrate
            presents.add(sid)
            records.append((id, ts, sid, value))
    return sids, hosts, guests, presents, records


def getSidToVid(sids, vidMapping):
    vp = vidMapping.pipeline(transaction=False)
    for sid in sids:
        vp.get(sid)
    vids = vp.execute()
    return dict(zip(sids, vids))


def getHostScore(value):
    if value > 600:
        return 2
    elif value > 500:
        return 3
    elif value > 400:
        return 4
    elif value > 300:
        return 5
    else:
        return 6

def getGuestScore(value):
    if value > 120:
        return 7
    else:
        return 8


def initConfig():
    configFile = os.path.dirname(os.path.realpath(__file__)) + '/product.ini'

    if not os.path.isfile(configFile):
        logger.error("config not exist:%s" % configFile)
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
    global COUNTER_NAMES

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

    COUNTER_KEYS = {'total': 0, 'freeze': 1, 'b5': 2, 'b4': 3, 'b3': 4, 'b2': 5, 'b1': 6, 'g2': 7, 'g1': 8, 'atotal': 9, 'afreeze': 10}
    COUNTER_NAMES = ['total', 'freeze', 'b5', 'b4', 'b3', 'b2', 'b1', 'g2', 'g1', 'atotal', 'afreeze']

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
            metricsCounter[tsKey] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
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
        afreezeRate = 1.0 * value[10] / value[9] if value[9] else 0
        opentsdbClient.log(freezerateMetric, freezeRate, timestamp=ts, vendor=vid, score=1)
        opentsdbClient.log(freezerateMetric, afreezeRate, timestamp=ts, vendor=vid, score=0)

        opentsdbClient.log(bitrateMetric, value[2], timestamp=ts, vendor=vid, score=5)
        opentsdbClient.log(bitrateMetric, value[3], timestamp=ts, vendor=vid, score=4)
        opentsdbClient.log(bitrateMetric, value[4], timestamp=ts, vendor=vid, score=3)
        opentsdbClient.log(bitrateMetric, value[5], timestamp=ts, vendor=vid, score=2)
        opentsdbClient.log(bitrateMetric, value[6], timestamp=ts, vendor=vid, score=1)
        opentsdbClient.log(bitrateMetric, value[7], timestamp=ts, vendor=vid, score=12)
        opentsdbClient.log(bitrateMetric, value[8], timestamp=ts, vendor=vid, score=11)


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

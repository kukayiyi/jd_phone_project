import happybase
import requests
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors
import traceback
import json


def do_product():
    producer = KafkaProducer(
        bootstrap_servers=['202.194.64.164:9092'])
    # 发送三条消息
    for i in range(0, 3):
        future = producer.send(
            'kafka_demo',
            key='count_num'.encode('utf-8'),  # 同一个key值，会被送至同一个分区
            # value=str(i))  #向分区1发送消息
            value='草泥马'.encode('utf-8'))
        print("send {}".format(str(i)))
        try:
            future.get(timeout=10)  # 监控是否发送成功
        except kafka_errors:  # 发送失败抛出kafka_errors
            traceback.format_exc()


def test_consume():
    consumer = KafkaConsumer(
        'kafka_demo',
        bootstrap_servers='202.194.64.164:9092',
    )
    for message in consumer:
        print("receive, key: {}, value: {}".format(
            json.loads(message.key.decode()),
            json.loads(message.value.decode())
        )
        )


if __name__ == '__main__':
    # do_product()
    # test_consume()

    # con_hbase = happybase.Connection(host="202.194.64.164", port=9090, transport="framed", protocol="compact")
    # con_hbase.open()
    # info_table = con_hbase.table("jd_phone_comments")
    # key = info_table.row("100008245587x033", columns=[b"comment:guid"])
    # if not key:
    #     print("111")
    # pass

    resp = requests.get(url="https://www.baidu.com", headers={"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36 "})
    js = resp.json()["testId"]
    pass

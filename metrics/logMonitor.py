import gzip
import base64
import json
import re
import boto3
import datetime
import os

client = boto3.client('cloudwatch')

FLUME_BLOCK_NUM_RE = re.compile(r"Committing \d+ logs up to block (\d+)")
LOG_COUNT_RE = re.compile(r"Committing (\d+) logs up to block \d+")


def numberFromRe(message, regex):
    m = regex.search(message)
    if not m:
        raise ValueError("Message does not match regex")
    return int(m.groups()[0])



def appendMetric(item, metricData, metricName, value, unit="None", stream=None):
    dimensions = [
        {
            'Name': 'clusterId',
            'Value': os.environ.get("CLUSTER_ID")
        },
    ]
    if stream:
        dimensions.append({
            'Name': 'instanceId',
            'Value': stream
        })
    metricData.append({
        'MetricName': metricName,
        'Dimensions': dimensions,
        'Timestamp': datetime.datetime.utcfromtimestamp(
            item["timestamp"] / 1000
        ),
        'Value': value,
        'Unit': unit,
    })


def flumeHandler(event, context):
    eventData = json.loads(gzip.decompress(base64.b64decode(
        event["awslogs"]["data"])
    ))
    for item in eventData["logEvents"]:
        metricData = []
        try:
            if "logs up to block" in item["message"]:
                appendMetric(item, metricData, "block",
                             numberFromRe(item["message"], FLUME_BLOCK_NUM_RE))
                appendMetric(item, metricData, "logCount",
                             numberFromRe(item["message"], LOG_COUNT_RE))
        except ValueError:
            pass
        if metricData:
            client.put_metric_data(
                Namespace='FlumeData',
                MetricData=metricData
            )

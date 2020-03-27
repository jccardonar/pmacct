#
#   pmacct (Promiscuous mode IP Accounting package)
#   pmacct is Copyright (C) 2003-2019 by Paolo Lucente
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 2 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#   Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
#   pmgrpcd and its components are Copyright (C) 2018-2019 by:
#
#   Matthias Arnold <matthias.arnold@swisscom.com>
#   Juan Camilo Cardona <jccardona82@gmail.com>
#   Thomas Graf <thomas.graf@swisscom.com>
#   Paolo Lucente <paolo@pmacct.net>
#
from export_pmgrpcd import Exporter
import ujson as json
import os
from confluent_kafka import Producer
import pickle
import itertools

def load_topics_file(file_json):
    '''
    The json file is an object. Keys are encoding paths.
    If the encoding path is absolute, just write the topic.
    TODO: maybe do something fancy later
    '''
    with open(file_json, 'r') as file_h:
        data_per_path = json.load(file_h)
    for key, value in data_per_path.items():
        if not isinstance(value, str):
            raise Exception(f"We only support files with simple key values as string, {value} is of a different type")
    return data_per_path


def create_topic(path):
    replacesments = set([':', '/'])
    rpath = path
    for ch in replacesments:
        rpath = rpath.replace(ch, ".")
    return rpath

class KafkaExporter(Exporter):
    def __init__(self, servers, topic, topic_per_encoding_path=None):
        if not servers:
            raise Exception(f"Kafka servers must be valid, got {servers}")
        if not topic:
            raise Exception(f"Kafka topic  must be valid, got {topic}")
        if not topic_per_encoding_path:
            topic_per_encoding_path = {}
        self.producer = Producer({"bootstrap.servers": servers})
        self.topic = topic
        self.topic_per_encoding_path = topic_per_encoding_path

    def get_topic(self, jsondata):
        # change this for the topic per encoding path.
        if "encoding_path" in jsondata["collector"]["data"]:
            encoding_path = jsondata["collector"]["data"]["encoding_path"]
            if encoding_path in self.topic_per_encoding_path:
                return self.topic_per_encoding_path[encoding_path]
        return self.topic

    def process_metric(self, datajsonstring):
        jsondata = json.loads(datajsonstring)
        topic = self.get_topic(jsondata)
        self.send(datajsonstring, topic)

    def send(self, text, topic=None):
        if topic is None:
            topic = self.topic
        self.producer.poll(0)
        self.producer.produce(topic, text.encode("utf-8"))


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
import export_pmgrpcd
import lib_pmgrpcd
from kafka_modules.kafka_avro_exporter import KafkaAvroExporter
from kafka_modules.kafka_simple_exporter import KafkaExporter, load_topics_file
from file_modules.file_producer import FileExporter
from lib_pmgrpcd import PMGRPCDLOG
from transformations import load_transformtions_from_file


def configure(config=None):
    """
    Setup all exporters
    """
    if config is None:
        config = lib_pmgrpcd.OPTIONS

    # Check for transfomrations
    if config.file_transformations:
        transformations = load_transformtions_from_file(config.file_transformations)
        if len(transformations) > 1:
            raise Exception("We only accept a single transformation right now")
        export_pmgrpcd.TRANSFORMATION = transformations[0]

    # Add the exporters
    if config.kafkaavro:
        if config.bsservers is None:
            raise Exception(f"Kafka servers  must be valid, got {config.bsservers}")
        if config.topic is None:
            raise Exception(f"Kafka topic  must be valid, got {config.topic}")
        kafka_avro_exporter = KafkaAvroExporter()
        export_pmgrpcd.EXPORTERS["kafkaavro"] = kafka_avro_exporter
    if config.kafkasimple:
        if config.bsservers is None:
            raise Exception(f"Kafka servers  must be valid, got {config.bsservers}")
        if config.topic is None:
            raise Exception(f"Kafka topic  must be valid, got {config.topic}")
        topic_per_encoding_path = {}
        if config.file_topic_per_encoding_path is not None:
            topic_per_encoding_path = load_topics_file(config.file_topic_per_encoding_path)
        exporter = KafkaExporter(config.bsservers, config.topic, topic_per_encoding_path)
        export_pmgrpcd.EXPORTERS["kafka"] = exporter
    if config.file_exporter_file is not None:
        exporter = FileExporter(config.file_exporter_file)
        export_pmgrpcd.EXPORTERS["file"] = exporter

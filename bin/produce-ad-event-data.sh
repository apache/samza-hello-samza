#!/bin/bash -e
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This script will prepare topics and start AdEventProducer which generates impression and click events

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=$(dirname ${DIR})
AD_PRODUCER_PATH=${BASE_DIR}/target/classes/samza/examples/join/AdEventProducer
SAMZA_LIB=${BASE_DIR}/deploy/samza/lib/

ZOOKEEPER=localhost:2181
KAFKA_BROKER=localhost:9092

echo Checking if required topics exist in kafka...
TOPICSRAW="ad-impression:1;ad-click:1;ad-imp-metadata:4;ad-clk-metadata:4;ad-event-error:1;ad-join:1;ad-imp-store-changelog:4;ad-clk-store-changelog:4"
IFS=';' read -a TOPICS <<< "$TOPICSRAW"
for i in "${!TOPICS[@]}"
do
  IFS=':' read -a TOPIC <<< "${TOPICS[$i]}"
  TOPIC_NAME=${TOPIC[0]}
  PARTITION_NUMBER=${TOPIC[1]}
  EXIST=$(${BASE_DIR}/deploy/kafka/bin/kafka-topics.sh --describe --topic ${TOPIC_NAME} --zookeeper ${ZOOKEEPER})
  if [ -z "${EXIST}" ]
  then
    echo -e topic "${TOPIC_NAME}" doesn\'t exists. Creating topic...
    ${BASE_DIR}/deploy/kafka/bin/kafka-topics.sh --create --topic ${TOPIC_NAME} --zookeeper ${ZOOKEEPER} --partitions ${PARTITION_NUMBER} --replication 1
  else
    echo Topic "${TOPIC_NAME}" already exists.
    read -a TOPIC_DESCRIPTION <<< "${EXIST}"
    for DESC in "${TOPIC_DESCRIPTION[@]}"
    do
      IFS=':' read -a KV <<< "${DESC}"
      if [ ${KV[0]} == 'PartitionCount' ]
      then
        if [ ${KV[1]} != $PARTITION_NUMBER  ]
        then
          echo Number of partitions for topic "${TOPIC_NAME}" is wrong. It should be ${PARTITION_NUMBER} instead of ${KV[1]}. Exiting.
          exit 0
        fi
      fi
    done
  fi
done

cd ${SAMZA_LIB}

echo -e "\nStarting AdEventProducer...\n"
java -cp "*" samza.examples.join.AdEventProducer
```
for the first time:
bin/grid bootstrap

after that: 
bin/grid start all

sh deploy/kafka/bin/kafka-topics.sh --create --topic imp-raw --zookeeper localhost:2181 --partitions 1 --replication 1
sh deploy/kafka/bin/kafka-topics.sh --create --topic bid-raw --zookeeper localhost:2181 --partitions 1 --replication 1
sh deploy/kafka/bin/kafka-topics.sh --create --topic imp-raw-partitioned --zookeeper localhost:2181 --partitions 4 --replication 1
sh deploy/kafka/bin/kafka-topics.sh --create --topic bid-raw-partitioned --zookeeper localhost:2181 --partitions 4 --replication 1
sh deploy/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181

mvn clean package
rm -rf deploy/samza
mkdir -p deploy/samza
tar -xvf ./target/hello-samza-0.10.0-dist.tar.gz -C deploy/samza

deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
    --config-path=file://$PWD/deploy/samza/config/magnetic-feed.properties
tail -100f deploy/yarn/logs/userlogs/application_XXXXXXXXXX_XXXX/container_XXXXXXXXXX_XXXX_XX_XXXXXX/{logs}

sh deploy/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic imp-raw

bin/grid stop all
```
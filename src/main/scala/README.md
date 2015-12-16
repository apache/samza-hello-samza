```
for the first time:
bin/grid bootstrap

sh deploy/kafka/bin/kafka-topics.sh --create --topic imp-raw --zookeeper localhost:2181 --partitions 1 --replication 1
sh deploy/kafka/bin/kafka-topics.sh --create --topic bid-raw --zookeeper localhost:2181 --partitions 1 --replication 1
sh deploy/kafka/bin/kafka-topics.sh --create --topic imp-raw-partitioned --zookeeper localhost:2181 --partitions 4 --replication 1
sh deploy/kafka/bin/kafka-topics.sh --create --topic bid-raw-partitioned --zookeeper localhost:2181 --partitions 4 --replication 1
sh deploy/kafka/bin/kafka-topics.sh --create --topic imp-error --zookeeper localhost:2181 --partitions 1 --replication 1
sh deploy/kafka/bin/kafka-topics.sh --create --topic bid-error --zookeeper localhost:2181 --partitions 1 --replication 1
sh deploy/kafka/bin/kafka-topics.sh --create --topic imp-bid-joined --zookeeper localhost:2181 --partitions 1 --replication 1
sh deploy/kafka/bin/kafka-topics.sh --create --topic imp-store-changelog --zookeeper localhost:2181 --partitions 4 --replication 1
sh deploy/kafka/bin/kafka-topics.sh --create --topic bid-store-changelog --zookeeper localhost:2181 --partitions 4 --replication 1
sh deploy/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181

after that: 
bin/grid start all

mvn clean package
rm -rf deploy/samza
mkdir -p deploy/samza
tar -xvf ./target/hello-samza-0.10.0-dist.tar.gz -C deploy/samza

deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
    --config-path=file://$PWD/deploy/samza/config/magnetic-feed.properties
deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
    --config-path=file://$PWD/deploy/samza/config/magnetic-join.properties
    
# Logs can be found:
tail -100f deploy/yarn/logs/userlogs/application_XXXXXXXXXX_XXXX/container_XXXXXXXXXX_XXXX_XX_XXXXXX/{logs}

sh deploy/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic imp-raw
sh deploy/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic bid-raw

sh deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic imp-raw-partitioned
sh deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic bid-raw-partitioned
sh deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic imp-error
sh deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic bid-error
sh deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic imp-bid-join

bin/grid stop all
```

ad log event:
	V=magnetic.domdex.com	t=[10/Dec/2015:00:00:00 +0000]	a=	u=-	c=c4706fc6df6f48b683d6aca71863f99f	m=GET	l=/ahtm	q=js=t&r=c&b=39634&c=57391&n=9468&id=650e33b95a1449705601&sz=728x90&s=onetravel.com&u=c4706fc6df6f48b683d6aca71863f99f&f=1&cat=00-00&ms=558536&kw=&kwcat=&dp=&a=VmjAfwAOX7AUNL2pBW_4_aHw4x_o6q1Wy3wCYA	s=200	b=2849	r=http://www.onetravel.com/	a0=2601:346:404:4e50:b090:77f3:4343:fbc1	ua=Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; InfoPath.3)	d=1570	rpt=ahtm	x=
bid log event:
1449705600	45799578e9064ca5b4e87af2aba77092	161.253.120.255	US	511	DC	20016	America/New_York	650e33b95a1449705601	5668c08c0008a5e80a1f1acb6c0f76fa	g	1		thegradcafe.com	728x90	1	00-00	1054641	9115	54663	38227				54663,52593,51249,51246,55928,50856,46309,52454,32235,50944		Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9	http://thegradcafe.com/survey/index.php	1	875	1000	1000	en	iab_tech		85	85	45	6500	45	15	25000	1000	600	1000	1000	1000	1000	1000	magnetic_ctr_variable_price	1.0	2.64151881627e-05								2015120920	-4.05492019653				2015120920		Safari	Mac_OS_X	00-00		0	1				70	n_a

kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic test

kafka-topics.sh --list --zookeeper zookeeper:2181

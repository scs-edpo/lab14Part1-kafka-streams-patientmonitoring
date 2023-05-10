echo "Waiting for Kafka to come online..."
cub kafka-ready -b kafka:9092 1 20
# create the pulse-events topic
kafka-topics --create --bootstrap-server kafka:9092 --topic pulse-events --replication-factor 1 --partitions 1
# create the body-temp-events topic
kafka-topics --create --bootstrap-server kafka:9092 --topic body-temp-events --replication-factor 1 --partitions 1
# create the combined-vitals topic
kafka-topics --create --bootstrap-server kafka:9092 --topic alerts --replication-factor 1 --partitions 1
sleep infinity
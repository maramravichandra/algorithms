package com.kafka.example

trait KafkaConstants {
  
	val KAFKA_BROKERS = "localhost:9092";
	val MESSAGE_COUNT=1000;
	val CLIENT_ID="client1";
	val TOPIC_NAME="demo";
	val GROUP_ID_CONFIG="consumerGroup1";
	val MAX_NO_MESSAGE_FOUND_COUNT=100;
	val OFFSET_RESET_LATEST="latest";
	val OFFSET_RESET_EARLIER="earliest";
	val MAX_POLL_RECORDS=1;
}
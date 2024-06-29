package io.producer.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class WikimediaChangeProducer {

	public static void main(String[] args) {

		Properties props = new Properties();

		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		String topic = "wikimedia.recentchange";

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

		ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, "first msg");

		kafkaProducer.send(producerRecord);

		kafkaProducer.flush();
		kafkaProducer.close();

	}

}

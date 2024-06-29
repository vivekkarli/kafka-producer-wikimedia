package io.producer.kafka.configurations;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GeneralProducerConfig {

	@Bean
	public KafkaProducer<String, String> getkafKafkaProducer() {

		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// properties for high throughput producer
		props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "5000");
		props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32kb

		return new KafkaProducer<>(props);

	}

}

package io.producer.kafka;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;

import io.producer.kafka.handlers.WikimediaEventHandler;

public class WikimediaChangeProducer {

	public static void main(String[] args) throws InterruptedException {

		Properties props = new Properties();

		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		String topic = "wikimedia.recentchange";

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

		BackgroundEventHandler eventHandler = new WikimediaEventHandler(kafkaProducer, topic);

		String sourceUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

		BackgroundEventSource eventSource = new BackgroundEventSource.Builder(eventHandler,
				new EventSource.Builder(ConnectStrategy.http(URI.create(sourceUrl)))).build();

		eventSource.start();

		TimeUnit.MILLISECONDS.sleep(5000);

	}

}

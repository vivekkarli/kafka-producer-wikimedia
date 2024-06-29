package io.producer.kafka.producers;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;

import io.producer.kafka.handlers.WikimediaEventHandler;

@Component
public class WikimediaChangeProducer {
	private final Logger log = LoggerFactory.getLogger(WikimediaChangeProducer.class);

	private KafkaProducer<String, String> kafkaProducer;

	@Autowired
	public WikimediaChangeProducer(KafkaProducer<String, String> kafkaProducer) {
		this.kafkaProducer = kafkaProducer;
	}

	@Async
	@Scheduled(initialDelay = 2000l)
	public void runWikimediaProducer() throws InterruptedException {
		log.info("Thread: {} is vitual?: {}",Thread.currentThread().getName(), Thread.currentThread().isVirtual());
		Thread.sleep(5000);
		
		String topic = "wikimedia.recentchange";

		BackgroundEventHandler eventHandler = new WikimediaEventHandler(kafkaProducer, topic);

		String sourceUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

		BackgroundEventSource eventSource = new BackgroundEventSource.Builder(eventHandler,
				new EventSource.Builder(ConnectStrategy.http(URI.create(sourceUrl)))).build();

		eventSource.start();
		
		TimeUnit.MILLISECONDS.sleep(1000);

	}

}

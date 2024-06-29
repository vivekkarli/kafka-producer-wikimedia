package io.producer.kafka.handlers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;

public class WikimediaEventHandler implements BackgroundEventHandler {

	private final Logger log = LoggerFactory.getLogger(WikimediaEventHandler.class);

	private KafkaProducer<String, String> kafkaProducer;
	private String topic;

	public WikimediaEventHandler(KafkaProducer<String, String> kafkaProducer, String topic) {

		this.kafkaProducer = kafkaProducer;
		this.topic = topic;

	}

	@Override
	public void onOpen() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void onClosed() throws Exception {
		kafkaProducer.close();

	}

	@Override
	public void onMessage(String event, MessageEvent messageEvent) throws Exception {
		log.info("Thread: {}, isVirtual:{} msg:{}",Thread.currentThread().getName(), Thread.currentThread().isVirtual(), messageEvent.getData());
		kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));

	}

	@Override
	public void onComment(String comment) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void onError(Throwable t) {
		log.error("error in reading stream", t);

	}

}

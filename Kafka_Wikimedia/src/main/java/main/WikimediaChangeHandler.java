package main;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

public class WikimediaChangeHandler implements EventHandler{
	private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

	KafkaProducer<String, String > kafkaProducer ;
	String topic;
	
	public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer , String topic) {
		this.kafkaProducer = kafkaProducer;
		this.topic = topic;
	}
	
	@Override
	public void onOpen() throws Exception {
	}

	@Override
	public void onClosed() throws Exception {
	}

	@Override
	public void onMessage(String event, MessageEvent messageEvent) throws Exception {
		// WHEN A STREAM HAS RECEIVE A MESSAGE
		log.info("004 :::::: DATA :::::: {} " , messageEvent.getData());
		kafkaProducer.send(new ProducerRecord<String, String>(topic, messageEvent.getData()));
		
	}

	@Override
	public void onComment(String comment) throws Exception {
	}

	@Override
	public void onError(Throwable t) {
		log.error(":::::: ERROR PRODUCER :::::: {} " , t.getMessage() , t);
		
	}

}

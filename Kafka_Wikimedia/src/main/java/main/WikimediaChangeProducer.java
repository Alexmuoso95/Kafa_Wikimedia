package main;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;


public class WikimediaChangeProducer {
	private static final Logger log = LoggerFactory.getLogger(WikimediaChangeProducer.class.getSimpleName());

	public static void main(String[] args) throws InterruptedException {
		log.info("001 Building Kafka Producer");
    	log.info("002 Configure");
    	Properties prop = new Properties();
    	prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
    	prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    	//Batch Compression
    	prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    	prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
    	prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(20));
    	
    	log.info("003 Create Producer");
    	KafkaProducer<String, String > producer = new KafkaProducer<>(prop);
    	
    	log.info("003 Create TOPIC manually");
    	String topic = "wikimedia.recenchange";
    	
    	EventHandler eventHandle = new WikimediaChangeHandler(producer,topic);
    	String url = "https://stream.wikimedia.org/v2/stream/recentchange" ;
    	EventSource.Builder builder = new EventSource.Builder(eventHandle, URI.create(url));
    	EventSource eventSource = builder.build();
    	
    	log.info("005 Start Event");
    	eventSource.start();
    	
    	log.info("006 BLOCK my thread (if not everything will finish) ");
    	//we produce for 10 minutes and block the program until then
    	TimeUnit.MINUTES.sleep(10);
	}
}

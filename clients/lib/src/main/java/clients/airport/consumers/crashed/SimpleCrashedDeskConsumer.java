package clients.airport.consumers.crashed;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import clients.airport.AirportProducer;
import clients.airport.AirportProducer.TerminalInfo;
import clients.airport.AirportProducer.TerminalInfoDeserializer;
import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;

/**
 * Consumer which will print out terminals that haven't a STATUS event in a while.
 */
public class SimpleCrashedDeskConsumer extends AbstractInteractiveShutdownConsumer {
	
	public static final String TOPIC_STATUS = "selfservice-status";
	
	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
		props.put("group.id", "crashed-desks-simple");

		// Kafka will auto-commit every 5s based on the last poll() call
		props.put("enable.auto.commit", "true");

		Map<Integer, Instant> lastHeartbeat = new TreeMap<>();

		// TODO: exercise
		try (KafkaConsumer<Integer, TerminalInfo> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new TerminalInfoDeserializer())) {
			consumer.subscribe(Collections.singleton(AirportProducer.TOPIC_STATUS));
			
			// add current time
			lastHeartbeat.put(0, Instant.now());
			
			while (!done) {
				ConsumerRecords<Integer, TerminalInfo> records = consumer.poll(Duration.ofSeconds(1));
				// when receive statue messages
				for (ConsumerRecord<Integer, TerminalInfo> record : records) {
					// update timestamp with the latest statue message
					lastHeartbeat.put(0, Instant.ofEpochMilli(record.timestamp()));
				}
				// If you don't get a message
				if(records.isEmpty()) {
					// check the last time of statue message and current time
					long duration =  Instant.now().getEpochSecond() - lastHeartbeat.get(0).getEpochSecond();	
					// System.out.println("get the message : Time : "+ duration +" seconds");
					if(duration >= 12) {
						break;
					}					
				}
			}
			System.out.println("Terminals has not sent a status message more than 12s. The desk is crashed");
		}
	}

	public static void main(String[] args) {
		new SimpleCrashedDeskConsumer().runUntilEnterIsPressed(System.in);
	}

}

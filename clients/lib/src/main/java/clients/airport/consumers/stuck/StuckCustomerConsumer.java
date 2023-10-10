package clients.airport.consumers.stuck;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import clients.airport.AirportProducer;
import clients.airport.AirportProducer.TerminalInfo;
import clients.airport.AirportProducer.TerminalInfoDeserializer;
import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;

/**
 * Detects started checkins which get stuck in the middle due to an OUT_OF_ORDER
 * event, and raises them as events.
 */
public class StuckCustomerConsumer extends AbstractInteractiveShutdownConsumer {

	private static final String TOPIC_STUCK_CUSTOMERS = "selfservice-stuck-customers";
	public static final String TOPIC_CANCELLED = "selfservice-cancelled";
	public static final String TOPIC_CHECKIN = "selfservice-checkin";
	public static final String TOPIC_COMPLETED = "selfservice-completed";
	public static final String TOPIC_OUTOFORDER = "selfservice-outoforder";

	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
		props.put("group.id", "stuck-customers-simple");
		props.put("enable.auto.commit", "true");

		Set<Integer> startedCheckins = new HashSet<>();

		try (KafkaConsumer<Integer, TerminalInfo> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new TerminalInfoDeserializer())) {
			consumer.subscribe(Arrays.asList(TOPIC_CANCELLED, TOPIC_CHECKIN, TOPIC_COMPLETED, TOPIC_OUTOFORDER));

			while (!done) {
				ConsumerRecords<Integer, TerminalInfo> records = consumer.poll(Duration.ofSeconds(1));
				for (ConsumerRecord<Integer, TerminalInfo> record : records) {
					switch(record.topic()) {
					case TOPIC_CHECKIN:
						startedCheckins.add(record.key());
						break;
					case TOPIC_CANCELLED:
						if(startedCheckins.contains(record.key())) {
							startedCheckins.remove(record.key());
						}
						break;
					case TOPIC_COMPLETED:
						if(startedCheckins.contains(record.key())) {
							startedCheckins.remove(record.key());
						}
						break;
					case TOPIC_OUTOFORDER: 
						System.out.printf("Terminal %d is out of order\n", record.key());
						break;
					}
				}
			}
		}
	}

	public static void main(String[] args) {
		new StuckCustomerConsumer().runUntilEnterIsPressed(System.in);
	}

}
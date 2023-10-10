package clients.airport.consumers.totals;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

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
 * Consumer that reports total numbers of started, completed, and cancelled
 * checkins. The first version is very simplistic and won't handle rebalancing.
 * This overall computation wouldn't scale well anyway, as it doesn't apply any
 * windows or split the input in any particular way.
 */
public class TotalCheckinsConsumer extends AbstractInteractiveShutdownConsumer {

	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
		props.put("group.id", "total-checkins");
		props.put("enable.auto.commit", "false");
		props.put("auto.offset.reset", "earliest");
		
		int started = 0, completed = 0, cancelled = 0;

		// TODO: exercise
		try (KafkaConsumer<Integer, TerminalInfo> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new TerminalInfoDeserializer())) {
			consumer.subscribe(Arrays.asList("selfservice-cancelled","selfservice-checkin", "selfservice-completed"));
			
			while (!done) {
				ConsumerRecords<Integer, TerminalInfo> records = consumer.poll(Duration.ofSeconds(1));
				for (ConsumerRecord<Integer, TerminalInfo> record : records) {
		
					if(record.topic().equals("selfservice-cancelled")) {
						cancelled++;
						System.out.printf("number of selfservice cancellation: %d, timestamps: %s\n", cancelled, Instant.ofEpochMilli(record.timestamp()).toString());
						
					}
					else if(record.topic().equals("selfservice-checkin")) {
						started++;
						System.out.printf("number of selfservice started: %d, timestamps: %s\n", started, Instant.ofEpochMilli(record.timestamp()).toString());
					}
					else if(record.topic().equals("selfservice-completed")) {
						completed++;
						System.out.printf("number of selfservice completed: %d, timestamps: %s\n", completed, Instant.ofEpochMilli(record.timestamp()).toString());
					}
				}
			}
		}

	}

	public static void main(String[] args) {
		new TotalCheckinsConsumer().runUntilEnterIsPressed(System.in);
	}

}

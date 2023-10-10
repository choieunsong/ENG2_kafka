package clients.airport.consumers.windows;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
 * Computes windowed checkin counts over each general area of the airport (defined
 * as the hundreds digit of the terminal ID, e.g. terminal 403 is in area 4).
 *
 * The first version compute counts correctly if we use more than one consumer in
 * the group, and it will forget events if we rebalance. We will fix these issues
 * later on.
 */
public class WindowedAreaCheckinsConsumer extends AbstractInteractiveShutdownConsumer {

	private Duration windowSize = Duration.ofSeconds(30);

	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
		props.put("group.id", "windowed-area-stats");
		props.put("enable.auto.commit", "true");

		Map<Integer, TimestampSlidingWindow> windowCheckinsByArea = new HashMap<>();

		// TODO: exercise (check the TimestampSlidingWindow tests for examples of how to use it)
		try (KafkaConsumer<Integer, TerminalInfo> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new TerminalInfoDeserializer())) {
			consumer.subscribe(Collections.singleton(AirportProducer.TOPIC_CHECKIN));
			System.out.println("Waiting for events...");

			while (!done) {
				ConsumerRecords<Integer, TerminalInfo> records = consumer.poll(Duration.ofSeconds(1));
				for (ConsumerRecord<Integer, TerminalInfo> record : records) {
					Instant eventTimestamp = Instant.ofEpochMilli(record.timestamp());
					int areaId = record.key() / 100;
					// System.out.println("record key: "+areaId + " / "+ record.key()+" eventTimeStamp: "+eventTimestamp + "record.timestamp: "+record.timestamp());
					
					// Update the TimestampSlidingWindow for the corresponding area
					TimestampSlidingWindow areaWindow = windowCheckinsByArea.getOrDefault(areaId, new TimestampSlidingWindow());
					areaWindow.add(eventTimestamp);
					windowCheckinsByArea.put(areaId, areaWindow);
					
					// Define your time window
					Instant windowEnd = Instant.now();
					Instant windowStart = windowEnd.minus(windowSize);
					
					// Get the count of checkins within the defined time window
					int checkinsWithinWindows = areaWindow.windowCount(windowStart, windowEnd);
					System.out.println("Area " + areaId + " has " + checkinsWithinWindows + " check-ins in the last 30 seconds");
				}
				
			}
		}
	}

	public static void main(String[] args) {
		new WindowedAreaCheckinsConsumer().runUntilEnterIsPressed(System.in);
	}

}

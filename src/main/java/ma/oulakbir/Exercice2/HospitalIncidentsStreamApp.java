package ma.oulakbir.Exercice2;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.utils.Bytes;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HospitalIncidentsStreamApp {

    public static void main(String[] args) {
        // Setup Kafka Streams properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "hospital-incidents-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Create a StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        // Step 1: Read from the 'hospital_incidents' topic
        KStream<String, String> incidentStream = builder.stream("hospital_incidents");

        // Step 2: Filter for critical incidents (severity = "Critique")
        KStream<String, String> criticalIncidents = incidentStream.filter((key, value) -> {
            String[] parts = value.split("\\|");
            String severity = parts[2];  // Extract severity field
            return severity.equals("Critique");
        });

        // Step 3: Extract service and timestamp from the incident data
        KStream<String, Long> serviceAndTimestamp = criticalIncidents.map((key, value) -> {
            String[] parts = value.split("\\|");
            String service = parts[1];  // Extract service field
            String timestamp = parts[4]; // Extract timestamp field
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                Date date = sdf.parse(timestamp);
                long timestampInMillis = date.getTime(); // Convert to milliseconds
                return KeyValue.pair(service, timestampInMillis); // Use service as key and timestamp as value
            } catch (Exception e) {
                e.printStackTrace();
                return KeyValue.pair("UnknownService", 0L); // Return default in case of error
            }
        });

        // Step 4: Group incidents by service
        KGroupedStream<String, Long> groupedByService = serviceAndTimestamp.groupByKey();

        // Step 5: Aggregate to calculate average time between incidents per service
        KTable<String, Double> averageTimeBetweenIncidents = groupedByService.aggregate(
                () -> 0.0, // Initial value: no time difference yet
                (service, newTime, aggregate) -> {
                    long newTimeMillis = newTime;
                    double oldAverage = aggregate;
                    // If there is no previous incident, skip calculation
                    if (oldAverage == 0.0) {
                        return 0.0; // No previous incident, skip calculation
                    } else {
                        double timeDifference = (newTimeMillis - oldAverage) / 1000.0; // Time difference in seconds
                        return (newTimeMillis + timeDifference) / 2; // Return new average
                    }
                },
                Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("incident-aggregates-store")
                        .withKeySerde(Serdes.String()) // Specify the key serializer (String)
                        .withValueSerde(Serdes.Double()) // Specify the value serializer (Double)
        );

        // Step 6: Format the time difference into hours and minutes
        averageTimeBetweenIncidents.toStream().mapValues(value -> {
            long averageTimeInSeconds = Math.round(value); // Round the Double to the nearest long
            long hours = TimeUnit.SECONDS.toHours(averageTimeInSeconds);
            long minutes = TimeUnit.SECONDS.toMinutes(averageTimeInSeconds) - TimeUnit.HOURS.toMinutes(hours);
            return String.format("service: \"%s\", averageTimeBetweenIncidents : \"%dh %dm\"",
                    "service_name_placeholder", hours, minutes);  // Placeholder for service name
        }).to("incident_time_analysis");  // Write to Kafka topic

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}

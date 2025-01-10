package ma.oulakbir.Exercice2;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import java.util.Properties;

public class HospitalIncidentsStreamAppQ1 {

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
            return severity.equals("Critique"); // Keep only critical incidents
        });

        // Step 3: Send the filtered incidents to a new topic 'critical_incidents'
        criticalIncidents.to("critical_incidents");  // Write to Kafka topic

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}


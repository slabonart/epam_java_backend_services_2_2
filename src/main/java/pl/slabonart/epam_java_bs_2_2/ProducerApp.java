package pl.slabonart.epam_java_bs_2_2;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import pl.slabonart.avro.Greeting;

import java.util.Properties;

public class ProducerApp {

    private final KafkaProducer<String, Greeting> kafkaProducer;
    private final String topic;

    public ProducerApp(String topic, String kafkaServer, String schemaRegistry) {
        this.topic = topic;

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put("schema.registry.url", schemaRegistry);

        this.kafkaProducer = new KafkaProducer<>(properties);
    }

    public void produce() {
        Greeting greeting = Greeting.newBuilder()
                .setGreet("Hello")
                .setTime(System.currentTimeMillis())
                .build();

        ProducerRecord<String, Greeting> record = new ProducerRecord<>(topic, "key", greeting);

        try {
            kafkaProducer.send(record);
        } catch (SerializationException e) {
            e.printStackTrace();
        }
    }

    public void shutDown() {
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}


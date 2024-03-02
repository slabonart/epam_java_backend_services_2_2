package pl.slabonart.epam_java_bs_2_2;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final String TOPIC = "avro-kafka";
    private static final String SCHEMA_REGISTRY = "http://localhost:8081";
    private static final String KAFKA_SERVER = "localhost:9092";

    public static void main(String[] args) {

        ProducerApp producerApp = new ProducerApp(TOPIC, KAFKA_SERVER, SCHEMA_REGISTRY);
        ConsumerApp consumerApp = new ConsumerApp(TOPIC, KAFKA_SERVER, SCHEMA_REGISTRY);

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        try {
            executor.scheduleAtFixedRate(() -> producerApp.produce(), 0, 1, TimeUnit.SECONDS);
            consumerApp.consume();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        } finally {
            producerApp.shutDown();
        }
    }
}
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class ConsumerDemo {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "topic-025-output";
        String groupId = "my-first-application-with-streams-starter";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create a consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        consumeMessages(logger, consumer);

    }

    private static void consumeMessages(Logger logger, KafkaConsumer<String, String> consumer) {
        int counter = 0;
        // poll for new data
        while(true){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

            for (ConsumerRecord<String, String> record : records){
                counter++;
                String myString = record.value();

                String [] fields = myString.split(",");
                Value value = new Value(fields[0],fields[1], fields[2],fields[3],fields[4],fields[5],fields[6],fields[7]);

                //logger.info("Key: " + record.key() + ", Value: " + value.getLatitude() + value.getLongtitude() + " " + value.getInfo());
                //logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                logger.info("[x" + counter +  ", y" + counter + "] " + "=" + value.getLatitude().replaceAll("latitude=", "") + ", " + value.getLongtitude().replaceAll("longtitude=", "").replaceAll("}",""));

            }
        }
    }

//    private static void consumeWithSampling(Logger logger, KafkaConsumer<String, String> consumer) {
//        int counter = 0;
//        // poll for new data
//        while(true){
//            ConsumerRecords<String, String> records =
//                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
//
//            double samplePercentage = 0.3;
//            int currentSubStreamSize = records.count();
//
//            int numberOfSamplesForCurrentSubStream = (int)(samplePercentage*currentSubStreamSize);
//
//            if(numberOfSamplesForCurrentSubStream>0) {
//
//                RandomSampling<ConsumerRecord<String, String>> rs = new WatermanSampling<>(numberOfSamplesForCurrentSubStream, new Random());
//                //rs.feed(IntStream.rangeClosed(1, 100).boxed().iterator());
//                rs.feed(records.iterator());
//                Collection<ConsumerRecord<String, String>> sample = rs.sample();
//
//                for (ConsumerRecord<String, String> record : sample){
//                    counter++;
//                    String myString = record.value();
//
//                    String [] fields = myString.split(",");
//                    Value value = new Value(fields[0],fields[1], fields[2],fields[3],fields[4],fields[5],fields[6],fields[7]);
//
//                    //logger.info("Key: " + record.key() + ", Value: " + value.getLatitude() + value.getLongtitude() + " " + value.getInfo());
//                    //logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
//                    logger.info("[x" + counter +  ", y" + counter + "] " + "=" + value.getLatitude().replaceAll("latitude=", "") + ", " + value.getLongtitude().replaceAll("longtitude=", "").replaceAll("}",""));
//                }
//            }
//        }
//    }
}

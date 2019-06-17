import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.time.Duration;
import java.util.*;

public class ConsumerDemo {

    final static String RAW_DATA_TOPIC = "topic-raw-data";
    final static String SAMPLED_DATA_TOPIC = "topic-sampled-data";

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "application-with-streams-topic-10";

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
        consumer.subscribe(Arrays.asList(RAW_DATA_TOPIC, SAMPLED_DATA_TOPIC));

        try {
            consumeMessages(logger, consumer);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void consumeMessages(Logger logger, KafkaConsumer<String, String> consumer) throws IOException{
        int counter = 0;
        int alertCounter = 0;
        int samplesOnRoute = 0;

        Map<String, Set<Coordinate>> rawCoordinatesPerTopic = new HashMap<>();
        Map<String, Set<Coordinate>> sampledCoordinatesPerTopic = new HashMap<>();

        boolean coordinateIsOnCorrectRoute = false;

        // poll for new data
        while(true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(7000000)); // new in Kafka 2.0.0
            //System.out.println("Polling! ");

            for (ConsumerRecord<String, String> record : records){
                String myTopic = record.topic();
                String myValue = record.value();
                counter++;

                String [] fields = myValue.split(",");
                Value value = new Value(fields[0],fields[1], fields[2],fields[3],fields[4],fields[5],fields[6],fields[7]);

                System.out.println(value.getBuslineId());

                String [] latitude = value.getLatitude().split("=");
                double x = Double.parseDouble(latitude[1]);

                String [] longtitude = value.getLongtitude().split("=");
                double y = Double.parseDouble(longtitude[1].replaceAll("}",""));

                Coordinate coordinate = new Coordinate(x, y);

                if (myTopic.equals(RAW_DATA_TOPIC)) {
                    Set<Coordinate> raw = rawCoordinatesPerTopic.get(value.getBuslineId());
                    if (raw == null) {
                        raw = new HashSet<>();
                        raw.add(coordinate);
                        rawCoordinatesPerTopic.put(value.getBuslineId(), raw);
                    }
                    else {
                        raw.add(coordinate);
                    }
                }
                else {
//                    Set<Coordinate> sampled = sampledCoordinatesPerTopic.get(value.getBuslineId());
//                    if (sampled == null) {
//                        sampled = new HashSet<>();
//                        sampled.add(coordinate);
//                        sampledCoordinatesPerTopic.put(value.getBuslineId(), sampled);
//                    } else {
//                        sampled.add(coordinate);
//                    }
                    if (rawCoordinatesPerTopic.get(value.getBuslineId()) == null) {
                        System.out.println("pigame na paroume sampled gia to topic " + myTopic + " kati pou den exoume raw ");
                    }
                    else {
                        coordinateIsOnCorrectRoute = rawCoordinatesPerTopic.get(value.getBuslineId()).stream().anyMatch(c -> c.equals(coordinate));

                        if (coordinateIsOnCorrectRoute) {
                            System.out.println("All good keep receiving sampled data. So far " + ++samplesOnRoute);
                        } else {
                            System.out.println("###############");
                            System.out.println("ALERT! Alert count so far is " + ++alertCounter);
                        }
                    }
                }

                //logger.info("Key: " + record.key() + ", Value: " + value.getLatitude() + value.getLongtitude() + " " + value.getInfo());
                //logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                logger.info("[x" + counter +  ", y" + counter + "] " + "=" + value.getLatitude().replaceAll("latitude=", "") + ", " + value.getLongtitude().replaceAll("longtitude=", "").replaceAll("}",""));
                //logger.info(counter + ":" + value.toString());

            }
        }
    }
}

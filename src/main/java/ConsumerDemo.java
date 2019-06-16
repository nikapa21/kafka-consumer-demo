import main.minigeo.MapWindow;
import main.minigeo.POI;
import main.minigeo.Point;
import main.minigeo.Segment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.QuickChart;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Color;
import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.awt.*;
import java.time.Duration;
import java.util.*;
import java.util.List;

public class ConsumerDemo {

    final static String RAW_DATA_TOPIC = "topic-10-raw-data";
    final static String SAMPLED_DATA_TOPIC = "topic-10-sampled-data";

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
        HashSet<Coordinate> rawCoordinateHashSet = new HashSet<>();
        Set<Coordinate> sampledCoordinateHashSet = new HashSet<>();
        boolean coordinateIsOnCorrectRoute = false;

//        List<Double> xCoordinates = new ArrayList<>();
//        List<Double> yCoordinates = new ArrayList<>();
        boolean flagRouteBigDataComing = true;

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

                String [] latitude = value.getLatitude().split("=");
                double x = Double.parseDouble(latitude[1]);

                String [] longtitude = value.getLongtitude().split("=");
                double y = Double.parseDouble(longtitude[1].replaceAll("}",""));

                Coordinate coordinate = new Coordinate(x, y);

                if (myTopic.equals(RAW_DATA_TOPIC)) {
                    rawCoordinateHashSet.add(coordinate);
                }
                else {
                    sampledCoordinateHashSet.add(coordinate);
                    coordinateIsOnCorrectRoute = rawCoordinateHashSet.stream().anyMatch(c -> c.equals(coordinate));

                    if (coordinateIsOnCorrectRoute) {
                        System.out.println("All good keep receiving sampled data");
                    }
                    else {
                        System.out.println("###############");
                        System.out.println("ALERT!");
                        System.out.println("###############");
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

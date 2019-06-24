import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class ConsumerDemo {

    final static String RAW_DATA_TOPIC = "topic-raw-data";
    final static String SAMPLED_DATA_TOPIC = "topic-sampled-data";


    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String bootstrapServers = "127.0.0.1:9090, 127.0.0.1:9091, 127.0.0.1:9092, 127.0.0.1:9093";
        String groupId = "application-with-sampling-all-topics";

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

        Output output = new Output(0, 0, 0, new ArrayList<>());

        Map<String, List<BusPosition>> rawCoordinatesPerBusLine = new HashMap<>();
        SimpleDateFormat dateFormat = new SimpleDateFormat("MMM  d yyyy hh:mm:ss:000a");
        Boolean stillProcessing = true;
        Boolean hasProcessed = false;
        long startProcessingMillis = System.currentTimeMillis();

        // poll for new data
        while(stillProcessing) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(2000));

            for (ConsumerRecord<String, String> record : records){
                startProcessingMillis = System.currentTimeMillis();
                String myTopic = record.topic();
                String myValue = record.value();

                String [] fields = myValue.split(",");
                Value value = new Value(fields[0], fields[1], fields[2],fields[3],fields[4],fields[5],fields[6],fields[7], fields[8]);
                String busLineId = value.getBuslineId();

                BusPosition busPosition = getBusPositionFromValue(value);

                if (myTopic.equals(RAW_DATA_TOPIC)) {
                    Util.initializeRawDataPerBusLine(output, rawCoordinatesPerBusLine, busLineId, busPosition);
                }
                else {
                    if (rawCoordinatesPerBusLine.get(busLineId) == null) {
                        logger.info("pigame na paroume sampled gia to topic " + myTopic + " kati pou den exoume raw ");
                    }
                    else {
                        hasProcessed = Util.processSampledData(logger, output, rawCoordinatesPerBusLine, dateFormat, busLineId, busPosition);
                    }
                }
            }
            stillProcessing = Util.checkIfStillProcessing(hasProcessed, startProcessingMillis);
        }
        Util.printStats(output);
    }



    private static BusPosition getBusPositionFromValue(Value value) {
        String [] latitude = value.getLatitude().split("=");
        double x = Double.parseDouble(latitude[1]);

        String [] longtitude = value.getLongtitude().split("=");
        double y = Double.parseDouble(longtitude[1].replaceAll("}",""));

        String [] ids = value.getId().split("=");
        int id = Integer.parseInt(ids[1]);

        return new BusPosition(id, value.getLineNumber(), value.getRouteCode(), value.getVehicleId(), x, y, value.getInfo());
    }
}

//logger.info("Key: " + record.key() + ", Value: " + value.getLatitude() + value.getLongtitude() + " " + value.getInfo());
//logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
//logger.info("[x" + counter +  ", y" + counter + "] " + "=" + value.getLatitude().replaceAll("latitude=", "") + ", " + value.getLongtitude().replaceAll("longtitude=", "").replaceAll("}",""));
//logger.info(counter + ":" + value.toString());
//logger.info("[x" + counter +  ", y" + counter + "] " + "=" + value.getLatitude().replaceAll("latitude=", "") + ", " + value.getLongtitude().replaceAll("longtitude=", "").replaceAll("}",""));

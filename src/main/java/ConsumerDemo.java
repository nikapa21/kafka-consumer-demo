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
    static SimpleDateFormat dateFormat = new SimpleDateFormat("MMM  d yyyy hh:mm:ss:000a");
    public static final long LONG_INTERVAL = 1000;

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "application-with-sampling-all-topics-predict";

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
        consumer.subscribe(Arrays.asList(SAMPLED_DATA_TOPIC));

        try {
            consumeMessages(logger, consumer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void consumeMessages(Logger logger, KafkaConsumer<String, String> consumer) throws IOException{

        List<ValueWithTopic> rawStigmata = new ArrayList<>();
        Map<String, List<BusPosition>> rawStigmataPerBusLine = new HashMap<>();
        Map<String, List<BusPosition>> eligibleStigmataForSamplingPerBusLine = new HashMap<>();
        List<ValueWithTopic> eligibleStigmataForSampling = new ArrayList<>();
        List<RouteVehicleStigma> linkedStigmata = new ArrayList<>();

        Map<String, List<Double>> listOfLossesPerBusLine = new HashMap<>();
        Map<String, List<Integer>> listOfSampledTimeDelaysToReachNextLocationPerBusLine = new HashMap<>();
        Map<String, Double> averageLossesPerBusLine = new HashMap<>();

        String busPositionsFile = "./Dataset/DS_project_dataset/busPositionsSixthOfMarchWithTopic.txt";
        String fileToBeSampled = "./Dataset/DS_project_dataset/busPositionsSixthOfMarchToBeSampledWithTopic.txt";

        // read file into stream, try-with-resources
        Util.readRawDataSet(rawStigmata, busPositionsFile);
        Output output = new Output(0, 0, 0, new ArrayList<>());
        Util.fromListToMapPerBusline(rawStigmata, rawStigmataPerBusLine, output);
        Util.findLinkedStigmataWithoutLongIntervals(rawStigmataPerBusLine, linkedStigmata);

        // read file into stream, try-with-resources
        Util.readSampleDataset(eligibleStigmataForSampling, fileToBeSampled);

        Util.fromListToMapPerBusline(eligibleStigmataForSampling, eligibleStigmataForSamplingPerBusLine, output);

        for (List<BusPosition> rawListToCheckVicinity : rawStigmataPerBusLine.values()) {

            int viceRawLocationCounter = 0;

            for(BusPosition stigma : rawListToCheckVicinity) {
                BusPosition nextStigma = null;
                List<Integer> listOfTimeDelaysToReachNextLocation = new ArrayList<>();
                int diff = 0;

                for (RouteVehicleStigma routeVehicleStigma : linkedStigmata) {
                    if (routeVehicleStigma.getBusPosition().equals(stigma) && routeVehicleStigma.getNext() != null) {
                        nextStigma = routeVehicleStigma.getNext();
                        diff = Util.getSecondsDiff(stigma, nextStigma);
                        if(diff <= LONG_INTERVAL) {
                            listOfTimeDelaysToReachNextLocation.add(diff);
                        }
                        break;
                    }
                }

                for (BusPosition busPosition : rawListToCheckVicinity) {
                    if (busPosition.isInTheVicinity(stigma) && !(busPosition.equals(stigma))) {

                        BusPosition viceBusPosition = busPosition;

                        for (RouteVehicleStigma routeVehicleStigma : linkedStigmata) {
                            if (routeVehicleStigma.getBusPosition().equals(viceBusPosition) && routeVehicleStigma.getNext() != null) {
                                viceRawLocationCounter++;

                                BusPosition nextBusPosition = routeVehicleStigma.getNext();

                                diff = Util.getSecondsDiff(viceBusPosition, nextBusPosition);
                                if(diff <= LONG_INTERVAL) {
                                    listOfTimeDelaysToReachNextLocation.add(diff);
                                }
                                break;
                            }
                        }
                    }
                }

//                logger.info("viceRawLocationCounter: " + viceRawLocationCounter);
//                logger.info("diff list size: " + listOfTimeDelaysToReachNextLocation.size());
//                logger.info(listOfTimeDelaysToReachNextLocation);

                double averageSeconds = Util.calculateAverageDelayFromOneLocationToTheNext(listOfTimeDelaysToReachNextLocation);
//                logger.info("Tha kanei peripou " + averageSeconds + " deuterolepta");

                for (RouteVehicleStigma routeVehicleStigma : linkedStigmata) {
                    if (routeVehicleStigma.getBusPosition().equals(stigma)) {
                        routeVehicleStigma.setAverageSecondsToNextLocation(averageSeconds);
                        if(averageSeconds > 1500) {
//                            logger.info("Extreme Average to Next location " + averageSeconds);
                        }
                        break;
                    }
                }
            }
        }

        int counter = 0;
        for (RouteVehicleStigma routeVehicleStigma : linkedStigmata) {
            if (routeVehicleStigma.getAverageSecondsToNextLocation()!=0.0) {
                counter++;
            }
        }
        logger.info("We have calculated average seconds for next location for: " + counter + " stigmata");

        Boolean stillProcessing = true;
        Boolean hasProcessed = false;
        long startProcessingMillis = System.currentTimeMillis();
        int allSampleCount = 0;
        int sampleViceCount = 0;


        // poll for new data
        while(stillProcessing) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(2000));

            for (ConsumerRecord<String, String> record : records) {
                allSampleCount++;
                startProcessingMillis = System.currentTimeMillis();
                String myValue = record.value();

                String [] fields = myValue.split(",");
                Value value = new Value(fields[0], fields[1], fields[2],fields[3],fields[4],fields[5],fields[6],fields[7], fields[8]);

                String [] busLines = value.getBuslineId().split("=");
                String busLine= busLines[1].replaceAll("'", "");

                BusPosition busPosition = Util.getBusPositionFromValue(value);

                for (RouteVehicleStigma routeVehicleStigma : linkedStigmata) {
                    if (routeVehicleStigma.getBusPosition().isInTheVicinity(busPosition) && routeVehicleStigma.getAverageSecondsToNextLocation() != 0.0) {

                        logger.info("Sampled vice count : {}, allSampleCount: {}" , ++sampleViceCount, allSampleCount);
                        logger.info("Bus " + busPosition + " will be at the next location in about " + routeVehicleStigma.getAverageSecondsToNextLocation() + " seconds");

                        Util.checkRealLoss(busLine, busPosition, eligibleStigmataForSamplingPerBusLine,
                                routeVehicleStigma.getAverageSecondsToNextLocation(),
                                listOfSampledTimeDelaysToReachNextLocationPerBusLine, listOfLossesPerBusLine);

                        break;
                    }
                    hasProcessed = Util.processSampledData(logger, output, rawStigmataPerBusLine, busLine, busPosition);
                }
            }
            stillProcessing = Util.checkIfStillProcessing(hasProcessed, startProcessingMillis);
        }
        logger.info("All Samples are: " + allSampleCount);

        for(String busLine : listOfLossesPerBusLine.keySet()) {
            double averageLoss = Util.calculateAverageLossFromOneLocationToTheNext(listOfLossesPerBusLine.get(busLine));
            averageLossesPerBusLine.put(busLine, averageLoss);
            double averageSampleRealTime = Util.calculateAverageDelayFromOneLocationToTheNext(listOfSampledTimeDelaysToReachNextLocationPerBusLine.get(busLine));
//            logger.info("Average loss for busLine " + busLine + " is " + averageLoss);
//            logger.info("Average seconds for busLine " + busLine + " for sampled data next location: " + averageSampleRealTime + " seconds");
        }
        logger.info("Average loss per busline list: " + averageLossesPerBusLine);
        Util.printStats(output);
    }
}

//logger.info("Key: " + record.key() + ", Value: " + value.getLatitude() + value.getLongtitude() + " " + value.getInfo());
//logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
//logger.info("[x" + counter +  ", y" + counter + "] " + "=" + value.getLatitude().replaceAll("latitude=", "") + ", " + value.getLongtitude().replaceAll("longtitude=", "").replaceAll("}",""));
//logger.info(counter + ":" + value.toString());
//logger.info("[x" + counter +  ", y" + counter + "] " + "=" + value.getLatitude().replaceAll("latitude=", "") + ", " + value.getLongtitude().replaceAll("longtitude=", "").replaceAll("}",""));

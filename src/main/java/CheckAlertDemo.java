import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

public class CheckAlertDemo {


    String addr;
    int port;
    static SimpleDateFormat dateFormat = new SimpleDateFormat("MMM  d yyyy hh:mm:ss:000a");
    public static final long LONG_INTERVAL = 1000;

    public CheckAlertDemo(String addr, int port) {
        this.addr = addr;
        this.port = port;
    }

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(CheckAlertDemo.class.getName());

        //find all the vehicle ids in one set
        String busPositionsFile = "./Dataset/DS_project_dataset/busPositionsSixthOfMarchWithTopic.txt";

        List<ValueWithTopic> rawStigmata = new ArrayList<>();
        Map<String, List<BusPosition>> rawStigmataPerBusLine = new HashMap<>();
        Map<String, List<BusPosition>> eligibleStigmataForSamplingPerBusLine = new HashMap<>();

        // read file into stream, try-with-resources
        try (Stream<String> stream = Files.lines(Paths.get(busPositionsFile))) {

            stream.map(line -> {
                String[] fields = line.split(",");
                ValueWithTopic value = new ValueWithTopic(fields[1], fields[2], fields[3], "", fields[0], fields[6], Double.parseDouble(fields[4]), Double.parseDouble(fields[5]));
                return value; })
                    .forEach(busPositionline -> rawStigmata.add(busPositionline));

        } catch(IOException e) {
            e.printStackTrace();
        }

        Output output = new Output(0, 0, 0, new ArrayList<>());
        List<RouteVehicleStigma> linkedStigmata = new ArrayList<>();

        fromListToMapPerBusline(rawStigmata, rawStigmataPerBusLine, output);

        for(List<BusPosition> stigmata : rawStigmataPerBusLine.values()) {
            for(BusPosition stigma : stigmata) {
                RouteVehicleStigma routeVehicleStigma = new RouteVehicleStigma(stigma, findNext(stigma, stigmata), null);
                filterOutLongIntervals(routeVehicleStigma);
                linkedStigmata.add(routeVehicleStigma);
            }
        }

        String fileToBeSampled = "./Dataset/DS_project_dataset/busPositionsSixthOfMarchToBeSampledWithTopic.txt";

        List<ValueWithTopic> eligibleStigmataForSampling = new ArrayList<>();

        // read file into stream, try-with-resources
        try (Stream<String> stream = Files.lines(Paths.get(fileToBeSampled))) {

            stream.map(line -> {
                String[] fields = line.split(",");
                ValueWithTopic valueWithTopic = new ValueWithTopic(fields[1], fields[2], fields[3], "", fields[0], fields[6], Double.parseDouble(fields[4]), Double.parseDouble(fields[5]));
                return valueWithTopic; })
                    .forEach(busPositionline -> eligibleStigmataForSampling.add(busPositionline));

        } catch(IOException e) {
            e.printStackTrace();
        }

        fromListToMapPerBusline(eligibleStigmataForSampling, eligibleStigmataForSamplingPerBusLine, output);

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

                System.out.println("Bus First Record First Location " + stigma);
                System.out.println("Bus First Record Second Location " + nextStigma);

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

                System.out.println("viceRawLocationCounter: " + viceRawLocationCounter);
//                System.out.println("diff list size: " + listOfTimeDelaysToReachNextLocation.size());
//                System.out.println(listOfTimeDelaysToReachNextLocation);

                double averageSeconds = calculateAverageDelayFromOneLocationToTheNext(listOfTimeDelaysToReachNextLocation);
                System.out.println("Tha kanei peripou " + averageSeconds + " deuterolepta");

                for (RouteVehicleStigma routeVehicleStigma : linkedStigmata) {
                    if (routeVehicleStigma.getBusPosition().equals(stigma)) {
                        routeVehicleStigma.setAverageSecondsToNextLocation(averageSeconds);
                        if(averageSeconds > 1500) {
                            System.out.println("Extreme Average to Next location " + averageSeconds);
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
        System.out.println("We have calculated average seconds for next location for: " + counter + " stigmata");

        int sampleViceCount = 0;
        double averageSeconds = 0;
        Map<String, Double> averageLossesPerBusLine = new HashMap<>();

//        List<BusPosition> allEligibleDataForSampling = new ArrayList<>();
        for(List<BusPosition> allEligibleDataForSampling : eligibleStigmataForSamplingPerBusLine.values()) {
            List<Double> listOfLosses = new ArrayList<>();
            List<Integer> listOfSampledTimeDelaysToReachNextLocation = new ArrayList<>();
            String busLine = "";

            for(BusPosition busPosition : allEligibleDataForSampling) {
//            if((busPosition.getRouteCode().equals("2006"))) {
                busLine = busPosition.getLineCode();
                for (RouteVehicleStigma routeVehicleStigma : linkedStigmata) {
                    if (routeVehicleStigma.getBusPosition().isInTheVicinity(busPosition) && routeVehicleStigma.getAverageSecondsToNextLocation() != 0.0) {
                        System.out.println("Sampled vice count : " + ++sampleViceCount);
                        System.out.println("Bus " + busPosition + " will be at the next location in about " + routeVehicleStigma.getAverageSecondsToNextLocation() + " seconds");

                        checkRealLoss(busPosition, allEligibleDataForSampling, routeVehicleStigma.getAverageSecondsToNextLocation(), listOfSampledTimeDelaysToReachNextLocation, listOfLosses);
                        break;
                    }
                }
//            }
            }
            double averageLoss = calculateAverageLossFromOneLocationToTheNext(listOfLosses);
            averageLossesPerBusLine.put(busLine, averageLoss);
            double averageSampleRealTime = calculateAverageDelayFromOneLocationToTheNext(listOfSampledTimeDelaysToReachNextLocation);
            System.out.println("Average loss is " + averageLoss);
            System.out.println("Average seconds for sampled data next location: " + averageSampleRealTime + " seconds");
        }
        System.out.println("Average loss per busline list: " + averageLossesPerBusLine);

        Util.printStats(output);
    }

    private static void filterOutLongIntervals(RouteVehicleStigma routeVehicleStigma) {

        if(routeVehicleStigma.getNext()!= null && Util.getSecondsDiff(routeVehicleStigma.getBusPosition(), routeVehicleStigma.getNext()) > LONG_INTERVAL) {
            routeVehicleStigma.setNext(null);
        }
    }

    private static void print040Stigmata(List<RouteVehicleStigma> linkedStigmata) {
        int counter = 0;
        for (RouteVehicleStigma routeVehicleStigma : linkedStigmata) {
            if (routeVehicleStigma.getBusPosition().getRouteCode().equals("2005")) {
                counter++;
            }
        }
        System.out.println("Next for 2006 route are: " + counter);
    }

    private static void printNextNull040Stigmata(List<RouteVehicleStigma> linkedStigmata) {
        int counter = 0;
        for (RouteVehicleStigma routeVehicleStigma : linkedStigmata) {
            if (routeVehicleStigma.getBusPosition().getRouteCode().equals("2005") && routeVehicleStigma.getNext() == null) {
                counter++;
            }
        }
        System.out.println("Next null for 2006 route are: " + counter);
    }

    private static BusPosition findNext(BusPosition stigma, List<BusPosition> stigmata) {
        // TODO to mono provlima edw einai o XRONOS
        for(BusPosition busPosition : stigmata.subList(stigmata.indexOf(stigma)+1, stigmata.size())) {
            if (busPosition.getRouteCode().equals(stigma.getRouteCode()) && busPosition.getVehicleId().equals(stigma.getVehicleId())) {
                return busPosition;
            }
        }
        return null;
    }

    private static double calculateAverageLossFromOneLocationToTheNext(List<Double> listOfLosses) {
        int totalSum = 0;
        for(Double loss : listOfLosses) {
            totalSum += loss;
        }
        if (listOfLosses.size() != 0)
            return totalSum/listOfLosses.size();
        else
            return 0.0;
    }

    private static void fromListToMapPerBusline(List<ValueWithTopic> stigmata, Map<String, List<BusPosition>> stigmataPerBusLine, Output output) {
        for(ValueWithTopic valueWithTopic : stigmata) {
            double x = valueWithTopic.getLatitude();
            double y = valueWithTopic.getLongtitude();
            String busLineId = valueWithTopic.getBuslineId();

            BusPosition busPosition = new BusPosition(valueWithTopic.getLineNumber(), valueWithTopic.getRouteCode(), valueWithTopic.getVehicleId(), x, y, valueWithTopic.getInfo());

            Util.initializeDataPerBusLine(output, stigmataPerBusLine, busLineId, busPosition);

        }
    }

    private static void checkRealLoss(BusPosition busPosition, List<BusPosition> allEligibleDataForSampling, double averageSeconds,List<Integer> listOfSampledTimeDelaysToReachNextLocation, List<Double> listOfLosses) {

        int indexOfViceBusPosition = allEligibleDataForSampling.indexOf(busPosition);

        for(BusPosition realNextBusPosition : allEligibleDataForSampling.subList(indexOfViceBusPosition, allEligibleDataForSampling.size())) {

            if(realNextBusPosition.getVehicleId().equals(busPosition.getVehicleId())
                    && (!realNextBusPosition.equals(busPosition))
                    && realNextBusPosition.getRouteCode().equals(busPosition.getRouteCode())) {

                System.out.println("Real Next Location of bus position " + realNextBusPosition);

                int diff = Util.getSecondsDiff(busPosition, realNextBusPosition);

                if(diff>LONG_INTERVAL) {
                    break;
                }

                double loss = Math.abs(diff - averageSeconds);
                listOfSampledTimeDelaysToReachNextLocation.add(diff);
                System.out.println("Real time to get to next position is: " + diff + " seconds");
                if(loss>10000) {
                    System.out.println("Extreme real loss " + loss);
                }
                System.out.println("Real Loss is : " + loss + " seconds");
                listOfLosses.add(loss);
                break;
            }
        }
    }

    private static double calculateAverageDelayFromOneLocationToTheNext(List<Integer> listOfTimeDelaysToReachNextLocation) {
        int totalSum = 0;
        for(Integer diff : listOfTimeDelaysToReachNextLocation){
            totalSum += diff;
        }
        if (listOfTimeDelaysToReachNextLocation.size() == 0) return 0.0;
        return (double) (totalSum/listOfTimeDelaysToReachNextLocation.size());
    }
}
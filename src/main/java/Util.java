import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Util {

    static SimpleDateFormat dateFormat = new SimpleDateFormat("MMM  d yyyy hh:mm:ss:000a");
    public static final long LONG_INTERVAL = 1000;

    public static boolean monitorSampledCoordinate(Map<String, List<BusPosition>> rawStigmataPerBusLine, String busLineId, BusPosition busPosition, List<BusPosition> existingRawDataPerBusLine) {
        boolean coordinateIsOnRoute = false;
        int index = 0;
        while (!coordinateIsOnRoute && index < existingRawDataPerBusLine.size()) {
            BusPosition currentLocation = existingRawDataPerBusLine.get(index);
            if (busPosition.isInTheVicinity(currentLocation) && !(currentLocation.equals(busPosition))) {
                coordinateIsOnRoute = true;
//                System.out.println("Sampled location vice to first bus record location: " + currentLocation);
//                Util.findNextLocation(rawStigmataPerBusLine, busLineId, busPosition, existingRawDataPerBusLine, index, currentLocation);
            }
            index++;
        }
        return coordinateIsOnRoute;
    }

    public static int checkForAlert(Logger logger, int alertCounter, BusPosition busPosition, boolean coordinateIsOnRoute) {
        if(!coordinateIsOnRoute) {
//            logger.info("###############");
//            logger.info("ALERT found for " + busPosition);
//            logger.info("Alert count so far is " + ++alertCounter);
        }
        return alertCounter;
    }

    public static void initializeDataPerBusLine(Output output, Map<String, List<BusPosition>> stigmataPerBusLine, String busLineId, BusPosition busPosition) {
        output.rawDataCounter++;
        List<BusPosition> raw = stigmataPerBusLine.get(busLineId);
        if (raw == null) {
            raw = new ArrayList<>();
            raw.add(busPosition);
            stigmataPerBusLine.put(busLineId, raw);
        }
        else {
            raw.add(busPosition);
        }
    }

    public static void findNextLocation(Map<String, List<BusPosition>> rawCoordinatesPerTopic, String busLineId, BusPosition busPosition, List<BusPosition> temporaryList, int index, BusPosition currentLocation) {
        try {
            for (int i = index + 1; i < rawCoordinatesPerTopic.get(busLineId).size(); i++) {
                BusPosition nextLocation = temporaryList.get(i);
                if (nextLocation.getRouteCode().equals(busPosition.getRouteCode())) {

                    int diff = Util.getSecondsDiff(currentLocation, nextLocation);

                    System.out.println("Current Location: " + currentLocation);
                    System.out.println("Next Location: " + nextLocation);
                    System.out.println("Ypologizoume oti tha einai stin epomeni stasi to polu se " + diff + " seconds!");
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("Current Location: " + currentLocation);
            System.out.println("Next Location: " + currentLocation);
            System.out.println("Ftasame sto telos tis diadromis! ");
        }
    }

    public static int getSecondsDiff(BusPosition currentLocation, BusPosition nextLocation) {
        Date firstParsedDate = null;
        try {
            firstParsedDate = dateFormat.parse(currentLocation.getTimeStampOfBusPosition().replaceAll("'", "").replaceAll(" info=", ""));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Date secondParsedDate = null;
        try {
            secondParsedDate = dateFormat.parse(nextLocation.getTimeStampOfBusPosition().replaceAll("'", "").replaceAll(" info=", ""));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return (Math.abs((int) secondParsedDate.getTime() - (int) firstParsedDate.getTime()))/1000;
    }

    public static Boolean processSampledData(Logger logger, Output output, Map<String, List<BusPosition>> rawStigmataPerBusLine, String busLineId, BusPosition busPosition) {
        Boolean hasProcessed;
        hasProcessed = true;
        output.sampleDataCounter++;
        output.sampledIds.add(busPosition.getId());

        List<BusPosition> existingRawDataPerBusLine = rawStigmataPerBusLine.get(busLineId);

        boolean coordinateIsOnRoute = Util.monitorSampledCoordinate(rawStigmataPerBusLine, busLineId, busPosition, existingRawDataPerBusLine);
        output.alertCounter = Util.checkForAlert(logger, output.alertCounter, busPosition, coordinateIsOnRoute);
        return hasProcessed;
    }

    public static void printStats(Output output) {
        System.out.println("Raw data counter: " + output.rawDataCounter);
        System.out.println("Sample data counter: " + output.sampleDataCounter);
        System.out.println("Alert counter: " + output.alertCounter);
//        System.out.println("Sampled ids " + output.sampledIds);
        final int interval = 2000;
//        for(int i=0;i<output.rawDataCounter;i=i+interval) {
//            int count = 0;
//            for(int sampleId : output.sampledIds) {
//                if (sampleId > i && sampleId < i+interval) {
//                    count++;
//                }
//            }
//            System.out.println("[" + i + "," + (int)(i+interval) + "] has " + count);
//        }
    }

    public static boolean checkIfStillProcessing(boolean hasProcessed, long startProcessingMillis) {
        if (hasProcessed == false) return true;
        if (System.currentTimeMillis() - startProcessingMillis < 3000) {
            System.out.println(System.currentTimeMillis() + " " + startProcessingMillis);
            return true;
        }
        else {
            System.out.println(System.currentTimeMillis() + " " + startProcessingMillis);
            return false;
        }
    }

    public static void findLinkedStigmataWithoutLongIntervals(Map<String, List<BusPosition>> rawStigmataPerBusLine, List<RouteVehicleStigma> linkedStigmata) {
        for(List<BusPosition> stigmata : rawStigmataPerBusLine.values()) {
            for(BusPosition stigma : stigmata) {
                RouteVehicleStigma routeVehicleStigma = new RouteVehicleStigma(stigma, findNext(stigma, stigmata), null);
                filterOutLongIntervals(routeVehicleStigma);
                linkedStigmata.add(routeVehicleStigma);
            }
        }
    }

    public static void readRawDataSet(List<ValueWithTopic> rawStigmata, String busPositionsFile) {
        try (Stream<String> stream = Files.lines(Paths.get(busPositionsFile))) {

            stream.map(line -> {
                String[] fields = line.split(",");
                ValueWithTopic value = new ValueWithTopic(fields[1], fields[2], fields[3], "", fields[0], fields[6], Double.parseDouble(fields[4]), Double.parseDouble(fields[5]));
                return value;
            })
                    .forEach(busPositionline -> rawStigmata.add(busPositionline));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readSampleDataset(List<ValueWithTopic> eligibleStigmataForSampling, String fileToBeSampled) {
        try (Stream<String> stream = Files.lines(Paths.get(fileToBeSampled))) {

            stream.map(line -> {
                String[] fields = line.split(",");
                ValueWithTopic valueWithTopic = new ValueWithTopic(fields[1], fields[2], fields[3], "", fields[0], fields[6], Double.parseDouble(fields[4]), Double.parseDouble(fields[5]));
                return valueWithTopic;
            })
                    .forEach(busPositionline -> eligibleStigmataForSampling.add(busPositionline));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static BusPosition getBusPositionFromValue(Value value) {
        String [] latitude = value.getLatitude().split("=");
        double x = Double.parseDouble(latitude[1]);

        String [] longtitude = value.getLongtitude().split("=");
        double y = Double.parseDouble(longtitude[1].replaceAll("}",""));
//
//        String [] ids = value.getId().split("=");
//        int id = Integer.parseInt(ids[1]);

        String [] lineNumbers = value.getLineNumber().split("=");
        String lineCode = lineNumbers[1].replaceAll("'", "");

        String [] routeCodes = value.getRouteCode().split("=");
        String routeCode = routeCodes[1].replaceAll("'", "");

        String [] vehicleIds = value.getVehicleId().split("=");
        String vehicleId = vehicleIds[1].replaceAll("'", "");

        String [] timeStamps = value.getInfo().split("=");
        String timeStamp = timeStamps[1].replaceAll("'", "");

        return new BusPosition(0, lineCode, routeCode, vehicleId, x, y, timeStamp);
    }

    public static void filterOutLongIntervals(RouteVehicleStigma routeVehicleStigma) {

        if(routeVehicleStigma.getNext()!= null && Util.getSecondsDiff(routeVehicleStigma.getBusPosition(), routeVehicleStigma.getNext()) > LONG_INTERVAL) {
            routeVehicleStigma.setNext(null);
        }
    }

    public static BusPosition findNext(BusPosition stigma, List<BusPosition> stigmata) {
        for(BusPosition busPosition : stigmata.subList(stigmata.indexOf(stigma)+1, stigmata.size())) {
            if (busPosition.getRouteCode().equals(stigma.getRouteCode()) && busPosition.getVehicleId().equals(stigma.getVehicleId())) {
                return busPosition;
            }
        }
        return null;
    }

    public static double calculateAverageLossFromOneLocationToTheNext(List<Double> listOfLosses) {
        int totalSum = 0;
        for(Double loss : listOfLosses) {
            totalSum += loss;
        }
        if (listOfLosses.size() != 0)
            return totalSum/listOfLosses.size();
        else
            return 0.0;
    }

    public static void fromListToMapPerBusline(List<ValueWithTopic> rawStigmata, Map<String, List<BusPosition>> stigmataPerBusLine, Output output) {
        for(ValueWithTopic valueWithTopic : rawStigmata) {
            double x = valueWithTopic.getLatitude();
            double y = valueWithTopic.getLongtitude();
            String busLineId = valueWithTopic.getBuslineId();

            BusPosition busPosition = new BusPosition(valueWithTopic.getLineNumber(), valueWithTopic.getRouteCode(), valueWithTopic.getVehicleId(), x, y, valueWithTopic.getInfo());

            Util.initializeDataPerBusLine(output, stigmataPerBusLine, busLineId, busPosition);
        }
    }

    public static void checkRealLoss(String busLine, BusPosition busPosition, Map<String, List<BusPosition>> eligibleStigmataForSamplingPerBusLine,
                                      double averageSeconds,Map<String, List<Integer>> listOfSampledTimeDelaysToReachNextLocationPerBusLine,
                                      Map<String, List<Double>> listOfLossesPerBusLine) {

        int indexOfViceBusPosition = eligibleStigmataForSamplingPerBusLine.get(busLine).indexOf(busPosition);
        List<Integer> listOfSampledTimeDelaysToReachNextLocation = listOfSampledTimeDelaysToReachNextLocationPerBusLine.get(busLine);
        List<Double> listOfLosses = listOfLossesPerBusLine.get(busLine);

        for(BusPosition realNextBusPosition : eligibleStigmataForSamplingPerBusLine.get(busLine).subList(indexOfViceBusPosition, eligibleStigmataForSamplingPerBusLine.get(busLine).size())) {

            if(realNextBusPosition.getVehicleId().equals(busPosition.getVehicleId())
                    && (!realNextBusPosition.equals(busPosition))
                    && realNextBusPosition.getRouteCode().equals(busPosition.getRouteCode())) {

                int diff = Util.getSecondsDiff(busPosition, realNextBusPosition);

                if(diff>LONG_INTERVAL) {
                    break;
                }

                double loss = Math.abs(diff - averageSeconds);
                if(listOfSampledTimeDelaysToReachNextLocation == null) {  listOfSampledTimeDelaysToReachNextLocation = new ArrayList<>();}
                listOfSampledTimeDelaysToReachNextLocation.add(diff);
                listOfSampledTimeDelaysToReachNextLocationPerBusLine.put(busLine, listOfSampledTimeDelaysToReachNextLocation);
//                logger.info("Real time to get to next position is: " + diff + " seconds");
                if(loss>10000) {
//                    logger.info("Extreme real loss " + loss);
                }
//                logger.info("Real Loss is : " + loss + " seconds");
                if(listOfLosses == null) listOfLosses = new ArrayList<>();
                listOfLosses.add(loss);
                listOfLossesPerBusLine.put(busLine, listOfLosses);
                break;
            }
        }
    }

    public static double calculateAverageDelayFromOneLocationToTheNext(List<Integer> listOfTimeDelaysToReachNextLocation) {
        int totalSum=0;
        for(Integer diff : listOfTimeDelaysToReachNextLocation){
            totalSum += diff;
        }
        if (listOfTimeDelaysToReachNextLocation.size() == 0) return 0.0;
        return (double) (totalSum/listOfTimeDelaysToReachNextLocation.size());
    }
}

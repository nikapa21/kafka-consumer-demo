import org.slf4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class Util {

    public static boolean monitorSampledCoordinate(Map<String, List<BusPosition>> rawCoordinatesPerBusLine, SimpleDateFormat dateFormat, String busLineId, BusPosition busPosition, List<BusPosition> existingRawDataPerBusLine) {
        boolean coordinateIsOnRoute = false;
        int index = 0;
        while (!coordinateIsOnRoute && index < existingRawDataPerBusLine.size()) {
            BusPosition currentLocation = existingRawDataPerBusLine.get(index);
            if (busPosition.isInTheVicinity(currentLocation)) {
                coordinateIsOnRoute = true;
                Util.findNextLocation(rawCoordinatesPerBusLine, dateFormat, busLineId, busPosition, existingRawDataPerBusLine, index, currentLocation);
            }
            index++;
        }
        return coordinateIsOnRoute;
    }

    public static int checkForAlert(Logger logger, int alertCounter, BusPosition busPosition, boolean coordinateIsOnRoute) {
        if(!coordinateIsOnRoute) {
            logger.info("###############");
            logger.info("ALERT found for " + busPosition);
            logger.info("Alert count so far is " + ++alertCounter);
        }
        return alertCounter;
    }

    public static void initializeRawDataPerBusLine(Output output, Map<String, List<BusPosition>> rawCoordinatesPerTopic, String busLineId, BusPosition busPosition) {
        output.rawDataCounter++;
        List<BusPosition> raw = rawCoordinatesPerTopic.get(busLineId);
        if (raw == null) {
            raw = new ArrayList<>();
            raw.add(busPosition);
            rawCoordinatesPerTopic.put(busLineId, raw);
        }
        else {
            raw.add(busPosition);
        }
    }

    public static void findNextLocation(Map<String, List<BusPosition>> rawCoordinatesPerTopic, SimpleDateFormat dateFormat, String busLineId, BusPosition busPosition, List<BusPosition> temporaryList, int index, BusPosition currentLocation) {
        try {
            for (int i = index + 1; i < rawCoordinatesPerTopic.get(busLineId).size(); i++) {
                BusPosition nextLocation = temporaryList.get(i);
                if (nextLocation.getRouteCode().equals(busPosition.getRouteCode())) {

                    int diff = Util.getSecondsDiff(dateFormat, currentLocation, nextLocation);

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

    public static int getSecondsDiff(SimpleDateFormat dateFormat, BusPosition currentLocation, BusPosition nextLocation) {
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

    public static Boolean processSampledData(Logger logger, Output output, Map<String, List<BusPosition>> rawCoordinatesPerBusLine, SimpleDateFormat dateFormat, String busLineId, BusPosition busPosition) {
        Boolean hasProcessed;
        hasProcessed = true;
        output.sampleDataCounter ++;
        output.sampledIds.add(busPosition.getId());

        List<BusPosition> existingRawDataPerBusLine = rawCoordinatesPerBusLine.get(busLineId);

        boolean coordinateIsOnRoute = Util.monitorSampledCoordinate(rawCoordinatesPerBusLine, dateFormat, busLineId, busPosition, existingRawDataPerBusLine);
        output.alertCounter = Util.checkForAlert(logger, output.alertCounter, busPosition, coordinateIsOnRoute);
        return hasProcessed;
    }

    public static void printStats(Output output) {
        System.out.println("Raw data counter: " + output.rawDataCounter);
        System.out.println("Sample data counter: " + output.sampleDataCounter);
        System.out.println("Alert counter: " + output.alertCounter);
        System.out.println("Sampled ids " + output.sampledIds);
        final int interval = 2000;
        for(int i=0;i<output.rawDataCounter;i=i+interval) {
            int count = 0;
            for(int sampleId : output.sampledIds) {
                if (sampleId > i && sampleId < i+interval) {
                    count++;
                }
            }
            System.out.println("[" + i + "," + (int)(i+interval) + "] has " + count);
        }
    }

    public static boolean checkIfStillProcessing(boolean hasProcessed, long startProcessingMillis) {
        if (hasProcessed == false) return true;
        if (System.currentTimeMillis() - startProcessingMillis < 1000) {
            System.out.println(System.currentTimeMillis() + " " + startProcessingMillis);
            return true;
        }
        else {
            System.out.println(System.currentTimeMillis() + " " + startProcessingMillis);
            return false;
        }
    }
}

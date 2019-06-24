import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

public class CheckAlertDemo {


    String addr;
    int port;

    public CheckAlertDemo(String addr, int port) {
        this.addr = addr;
        this.port = port;
    }

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(CheckAlertDemo.class.getName());

        //find all the vehicle ids in one set
        String busPositionFile = args[0];
        String busPositionsFile = "./Dataset/DS_project_dataset/"+busPositionFile;

        List<ValueWithTopic> publisherBusRecords = new ArrayList<>();
        Map<String, List<BusPosition>> rawCoordinatesPerBusLine = new HashMap<>();

        SimpleDateFormat dateFormat = new SimpleDateFormat("MMM  d yyyy hh:mm:ss:000a");

        //String busPositionsFile = "C:\\Users\\nikos\\workspace\\aueb\\distributed systems\\ds-project-2019\\Dataset\\DS_project_dataset\\busPositionsNew.txt";

        // read file into stream, try-with-resources
        try (Stream<String> stream = Files.lines(Paths.get(busPositionsFile))) {

            stream.map(line -> {
                String[] fields = line.split(",");
                ValueWithTopic value = new ValueWithTopic(fields[1], fields[2], fields[3], "", fields[0], fields[6], Double.parseDouble(fields[4]), Double.parseDouble(fields[5]));
                return value; })
                    .forEach(busPositionline -> publisherBusRecords.add(busPositionline));

        } catch(IOException e) {
            e.printStackTrace();
        }

        Output output = new Output(0, 0, 0, new ArrayList<>());

        for(ValueWithTopic valueWithTopic : publisherBusRecords) {
            double x = valueWithTopic.getLatitude();
            double y = valueWithTopic.getLongtitude();
            String busLineId = valueWithTopic.getBuslineId();

            BusPosition busPosition = new BusPosition(valueWithTopic.getLineNumber(), valueWithTopic.getRouteCode(), valueWithTopic.getVehicleId(), x, y, valueWithTopic.getInfo());

            Util.initializeRawDataPerBusLine(output, rawCoordinatesPerBusLine, busLineId, busPosition);

        }

        String alertFile = "./Dataset/DS_project_dataset/busAlertWithTopic.txt";

        List<ValueWithTopic> alertFileRecords = new ArrayList<>();
        //String busPositionsFile = "C:\\Users\\nikos\\workspace\\aueb\\distributed systems\\ds-project-2019\\Dataset\\DS_project_dataset\\busPositionsNew.txt";

        // read file into stream, try-with-resources
        try (Stream<String> stream = Files.lines(Paths.get(alertFile))) {

            stream.map(line -> {
                String[] fields = line.split(",");
                ValueWithTopic valueWithTopic = new ValueWithTopic(fields[1], fields[2], fields[3], "", fields[0], fields[6], Double.parseDouble(fields[4]), Double.parseDouble(fields[5]));
                return valueWithTopic; })
                    .forEach(busPositionline -> alertFileRecords.add(busPositionline));

        } catch(IOException e) {
            e.printStackTrace();
        }


        for(ValueWithTopic valueWithTopic : alertFileRecords) {
            double x = valueWithTopic.getLatitude();
            double y = valueWithTopic.getLongtitude();
            String busLineId = valueWithTopic.getBuslineId();

            BusPosition busPosition = new BusPosition(valueWithTopic.getLineNumber(), valueWithTopic.getRouteCode(), valueWithTopic.getVehicleId(), x, y, valueWithTopic.getInfo());

            if (rawCoordinatesPerBusLine.get(busLineId) == null) {
                System.out.println("pigame na paroume sampled gia to topic kati pou den exoume raw ");
            }
            else {
                Util.processSampledData(logger, output, rawCoordinatesPerBusLine, dateFormat, busLineId, busPosition);
            }
        }
        Util.printStats(output);
    }
}
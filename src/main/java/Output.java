import java.util.ArrayList;
import java.util.List;

public class Output {
    public Integer sampleDataCounter = 0;
    public Integer rawDataCounter = 0;
    public Integer alertCounter = 0;
    public List<Integer> sampledIds = new ArrayList<>();

    public Output(Integer sampleDataCounter, Integer rawDataCounter, Integer alertCounter, List<Integer> sampledIds) {
        this.sampleDataCounter = sampleDataCounter;
        this.rawDataCounter = rawDataCounter;
        this.alertCounter = alertCounter;
        this.sampledIds = sampledIds;
    }
}

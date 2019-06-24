import java.io.Serializable;

public class ValueWithTopic implements Serializable {
    //private Bus bus;
    private int id;
    private String lineNumber;
    private String routeCode;
    private String vehicleId;
    private String lineName;
    private String buslineId;
    private String info;
    private double latitude;
    private double longtitude;

    public ValueWithTopic(String lineNumber, String routeCode, String vehicleId, String lineName, String buslineId, String info, double latitude, double longtitude) {
        this.lineNumber = lineNumber;
        this.routeCode = routeCode;
        this.vehicleId = vehicleId;
        this.lineName = lineName;
        this.buslineId = buslineId;
        this.info = info;
        this.latitude = latitude;
        this.longtitude = longtitude;
    }

    public ValueWithTopic(int id, String lineNumber, String routeCode, String vehicleId, String lineName, String buslineId, String info, double latitude, double longtitude) {
        this.id = id;
        this.lineNumber = lineNumber;
        this.routeCode = routeCode;
        this.vehicleId = vehicleId;
        this.lineName = lineName;
        this.buslineId = buslineId;
        this.info = info;
        this.latitude = latitude;
        this.longtitude = longtitude;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getLineNumber() {
        return lineNumber;
    }

    public void setLineNumber(String lineNumber) {
        this.lineNumber = lineNumber;
    }

    public String getRouteCode() {
        return routeCode;
    }

    public void setRouteCode(String routeCode) {
        this.routeCode = routeCode;
    }

    public String getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(String vehicleId) {
        this.vehicleId = vehicleId;
    }

    public String getLineName() {
        return lineName;
    }

    public void setLineName(String lineName) {
        this.lineName = lineName;
    }

    public String getBuslineId() {
        return buslineId;
    }

    public void setBuslineId(String buslineId) {
        this.buslineId = buslineId;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongtitude() {
        return longtitude;
    }

    public void setLongtitude(double longtitude) {
        this.longtitude = longtitude;
    }

    @Override
    public String toString() {
        return "Value{" +
                "id=" + id +
                ", lineNumber='" + lineNumber + '\'' +
                ", routeCode='" + routeCode + '\'' +
                ", vehicleId='" + vehicleId + '\'' +
                ", lineName='" + lineName + '\'' +
                ", buslineId='" + buslineId + '\'' +
                ", info='" + info + '\'' +
                ", latitude=" + latitude +
                ", longtitude=" + longtitude +
                '}';
    }
}

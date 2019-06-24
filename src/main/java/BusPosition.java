public class BusPosition {

    final static int MAX_ALLOWED_DEVIATION = 2;

    private int id;
    private String lineCode;
    private String routeCode;
    private String vehicleId;
    private double latitude;
    private double longtitude;
    private String timeStampOfBusPosition;

    public BusPosition(int id, String lineCode, String routeCode, String vehicleId, double latitude, double longtitude, String timeStampOfBusPosition) {
        this.id = id;
        this.lineCode = lineCode;
        this.routeCode = routeCode;
        this.vehicleId = vehicleId;
        this.latitude = latitude;
        this.longtitude = longtitude;
        this.timeStampOfBusPosition = timeStampOfBusPosition;
    }

    public BusPosition(String lineCode, String routeCode, String vehicleId, double latitude, double longtitude, String timeStampOfBusPosition) {
        this.lineCode = lineCode;
        this.routeCode = routeCode;
        this.vehicleId = vehicleId;
        this.latitude = latitude;
        this.longtitude = longtitude;
        this.timeStampOfBusPosition = timeStampOfBusPosition;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getLineCode() {
        return lineCode;
    }

    public void setLineCode(String lineCode) {
        this.lineCode = lineCode;
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

    public String getTimeStampOfBusPosition() {
        return timeStampOfBusPosition;
    }

    public void setTimeStampOfBusPosition(String timeStampOfBusPosition) {
        this.timeStampOfBusPosition = timeStampOfBusPosition;
    }

    public boolean isInTheVicinity(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BusPosition that = (BusPosition) o;

        boolean compareLatitude = false;
        boolean compareLongtitude = false;

        int thatLatAsInt = (int) (that.latitude * 10000);
        int thatLonAsInt = (int) (that.longtitude * 10000);

        int thisLatitudeAsInt = (int) (this.latitude * 10000);
        int thisLongtitudeAsInt = (int) (this.longtitude * 10000);

        if (Math.abs(thatLatAsInt-thisLatitudeAsInt) <= MAX_ALLOWED_DEVIATION) {
            compareLatitude = true;
        }

        if (Math.abs(thatLonAsInt-thisLongtitudeAsInt) <= MAX_ALLOWED_DEVIATION) {
            compareLongtitude = true;
        }

        boolean routeIsTheSame = that.routeCode.equals(this.routeCode);

        return  compareLatitude && compareLongtitude && routeIsTheSame;
    }

    @Override
    public String toString() {
        return "BusPosition{" +
                "id=" + id +
                ", lineCode='" + lineCode + '\'' +
                ", routeCode='" + routeCode + '\'' +
                ", vehicleId='" + vehicleId + '\'' +
                ", latitude=" + latitude +
                ", longtitude=" + longtitude +
                ", timeStampOfBusPosition='" + timeStampOfBusPosition + '\'' +
                '}';
    }
}

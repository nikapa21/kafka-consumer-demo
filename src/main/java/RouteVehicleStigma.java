public class RouteVehicleStigma {
    BusPosition busPosition;
    BusPosition next;
    BusPosition previous;
    double averageSecondsToNextLocation;

    public RouteVehicleStigma(BusPosition busPosition, BusPosition next, BusPosition previous) {
        this.busPosition = busPosition;
        this.next = next;
        this.previous = previous;
    }

    public BusPosition getBusPosition() {
        return busPosition;
    }

    public void setBusPosition(BusPosition busPosition) {
        this.busPosition = busPosition;
    }

    public BusPosition getNext() {
        return next;
    }

    public void setNext(BusPosition next) {
        this.next = next;
    }

    public BusPosition getPrevious() {
        return previous;
    }

    public void setPrevious(BusPosition previous) {
        this.previous = previous;
    }

    public double getAverageSecondsToNextLocation() {
        return averageSecondsToNextLocation;
    }

    public void setAverageSecondsToNextLocation(double averageSecondsToNextLocation) {
        this.averageSecondsToNextLocation = averageSecondsToNextLocation;
    }

    @Override
    public String toString() {
        return "RouteVehicleStigma{" +
                "busPosition=" + busPosition +
                ", next=" + next +
                ", previous=" + previous +
                '}';
    }
}

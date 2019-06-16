import java.util.Objects;

public class Coordinate {

    final static int MAX_ALLOWED_DEVIATION = 1;

    double latitude;
    double longtitude;

    public Coordinate(Double latitude, Double longtitude) {
        this.latitude = latitude;
        this.longtitude = longtitude;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Coordinate that = (Coordinate) o;

        boolean compareLatitude = false;
        boolean compareLongtitude = false;


        int thatLatAsInt = (int) (that.latitude * 1000);
        int thatLonAsInt = (int) (that.longtitude * 1000);

        int thisLatitudeAsInt = (int) (this.latitude * 1000);
        int thisLongtitudeAsInt = (int) (this.longtitude * 1000);

//        System.out.println("Comparing " + thatLatAsInt + " with " + latitudeAsInt + " result is ");
//        System.out.println("Comparing " + thatLonAsInt + " with " + longtitudeAsInt+ " result is ");

        if (Math.abs(thatLatAsInt-thisLatitudeAsInt) <= MAX_ALLOWED_DEVIATION) {
            compareLatitude = true;
        }

        if (Math.abs(thatLonAsInt-thisLongtitudeAsInt) <= MAX_ALLOWED_DEVIATION) {
            compareLongtitude = true;
        }

        return  compareLatitude && compareLongtitude;
    }

}

import java.io.Serializable;

public class Poi implements Serializable {

    private int id;
    private String name;
    private double latitude;
    private double longitude;
    private String category;

    public Poi() {}

    public Poi(int id, String name, double latitude, double longitude, String category) {
        this.setId(id);
        this.setName(name);
        this.setLatitude(latitude);
        this.setLongitude(longitude);
        this.category = category;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

}

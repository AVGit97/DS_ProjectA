import test.ds_project.poi.Poi;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

public class Client extends Thread {

    private int id;
    private double latitude, longitude;

    private Socket requestSocket = null;
    private ObjectInputStream in = null;
    private ObjectOutputStream out = null;

    public Client() {
        this(0, 0, 0);
    }

    public Client(int id, double latitude, double longitude) {
        this.id = id;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public void initialize() {

        String hostIP = "192.168.1.2";
        System.out.println("Trying to connect to " + hostIP);

        try {
            requestSocket = new Socket(hostIP, 4321);
            System.out.println("Successfully connected to " + hostIP);

            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            // send information to server
            out.writeInt(id);
            out.flush();

            out.writeDouble(latitude);
            out.flush();

            out.writeDouble(longitude);
            out.flush();

            // wait for response
            int k = in.readInt();
            double range = in.readDouble();

            ArrayList<Poi> listFromServer = (ArrayList<Poi>) in.readObject();

            ArrayList<Poi> bestLocalPois;
            if (listFromServer != null) {
                bestLocalPois = new ArrayList<Poi>(listFromServer);
                System.out.println("Here are the best " + k + " local pois in a " + range + "km range:");
                for (Poi p:
                        bestLocalPois) {
                    System.out.println("Poi " + p.getId() + ": at " + p.getLatitude() + ", " + p.getLongitude());
                }
            } else {
                System.out.println("There is nothing interesting here for you!");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
                out.close();
                requestSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public void run() { initialize(); }

}

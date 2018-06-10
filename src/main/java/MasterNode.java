import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.json.JSONException;
import org.json.JSONObject;
import test.ds_project.poi.Poi;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class MasterNode implements Master {
    private RealMatrix YMatrix;
    private RealMatrix XMatrix;
    private ServerSocket providerSocket;

    private ArrayList<Poi> pois = new ArrayList<Poi>();

    private ArrayList<ObjectOutputStream> out = new ArrayList<ObjectOutputStream>();
    private ArrayList<ObjectInputStream> in = new ArrayList<ObjectInputStream>();

    private ArrayList<Integer> coresPerWorker = new ArrayList<Integer>();
    private ArrayList<Long> freeMemPerWorker = new ArrayList<Long>();

    private ArrayList<Double> workerValue = new ArrayList<Double>();

    // set the number of workers
    private int workers;

    private double sum; // workers' value sum

    private int currentStartRowX;
    private int currentStartRowY;

    private final int epochs = 100;

    private double lambda = .1;

    private RealMatrix C, P;

    // range in km
    private double range = 1;

    // k: number of best pois to be returned
    private int k = 10;

    /**
     * @param workers the number of the workers
     */
    public MasterNode(int workers) {
        this.workers = workers;
    }

    public void initialize() {

        // Import our matrix from file
        Scanner sc = null;
        try {
//            sc = new Scanner(new File("input_matrix_no_zeros.csv"));
            sc = new Scanner(new File("input_matrix_no_zeros_final.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String line;
        ArrayList<String[]> elements = new ArrayList<String[]>();

        if (sc != null) {
            while (sc.hasNext()) {
                line = sc.nextLine();
                elements.add(line.split(", "));
            }
        } else {
            System.out.println("sc == null!");
            return;
        }

        int rMax = 0, cMax = 0;
        for (String[] element : elements) {
            rMax = Integer.parseInt(element[0]) > rMax ? Integer.parseInt(element[0]) : rMax;
            cMax = Integer.parseInt(element[1]) > cMax ? Integer.parseInt(element[1]) : cMax;
        }

        final int rows = rMax + 1;
        final int columns = cMax + 1;

        System.out.println("rows: " + rows + "\ncolumns: " + columns);

//      Change 1 to the percentage of the matrix you want to use
        final int rows_sub = (int) Math.round(.1 * rows);
        final int columns_sub = (int) Math.round(.1 * columns);

        // Create R Matrix
        RealMatrix R = MatrixUtils.createRealMatrix(rows, columns);
        for (String[] element : elements) {
            R.setEntry(Integer.parseInt(element[0]), Integer.parseInt(element[1]), Double.parseDouble(element[2]));
        }

        R = R.getSubMatrix(0, rows_sub - 1, 0, columns_sub - 1);
        System.out.println("R matrix created. (" + R.getRowDimension() + " x " + R.getColumnDimension() + ")");

        // Create pois ArrayList
        String jsonData = readFile("POIs.json");
        JSONObject allPois;
        try {
            allPois = new JSONObject(jsonData);
            JSONObject currentPoi;
            String name, category;
            double latitude;
            double longitude;
//            for (int i = 0; i < R.getColumnDimension(); i++) {
            for (int i = 0; i < 1692; i++) {
                currentPoi = new JSONObject(allPois.getString(String.valueOf(i)));
                name = currentPoi.getString("POI_name");
                category = currentPoi.getString("POI_category_id");
                latitude = currentPoi.getDouble("latitude");
                longitude = currentPoi.getDouble("longitude");
                pois.add(new Poi(i, name, latitude, longitude, category));
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }

        // Calculate P Matrix
        P = MatrixUtils.createRealMatrix(rows, columns);
        calculatePMatrix(R);

        P = P.getSubMatrix(0, rows_sub - 1, 0, columns_sub - 1);
        System.out.println("P matrix created. (" + P.getRowDimension() + " x " + P.getColumnDimension() + ")");

        // Calculate C Matrix
        C = MatrixUtils.createRealMatrix(rows, columns);
        calculateCMatrix(40, R);

        C = C.getSubMatrix(0, rows_sub - 1, 0, columns_sub - 1);
        System.out.println("C matrix created. (" + C.getRowDimension() + " x " + C.getColumnDimension() + ")");

        RandomGenerator rGen = new JDKRandomGenerator();
        rGen.setSeed(1);

        // define f
        int f = Math.max(R.getRowDimension(), R.getColumnDimension()) / 20;
        f = 20;
        System.out.println("f = " + f);

        XMatrix = MatrixUtils.createRealMatrix(R.getRowDimension(), f);
        System.out.println("X: " + XMatrix.getRowDimension() + " x " + XMatrix.getColumnDimension());

        YMatrix = MatrixUtils.createRealMatrix(R.getColumnDimension(), f);
        System.out.println("Y: " + YMatrix.getRowDimension() + " x " + YMatrix.getColumnDimension());

        // initialize XMatrix with random values
        for (int i = 0; i < XMatrix.getRowDimension(); i++) {
            for (int j = 0; j < f; j++) {
                XMatrix.setEntry(i, j, rGen.nextDouble());
            }
        }

        // initialize YMatrix with random values
        for (int i = 0; i < YMatrix.getRowDimension(); i++) {
            for (int j = 0; j < f; j++) {
                YMatrix.setEntry(i, j, rGen.nextDouble());
            }
        }

        try {

            providerSocket = new ServerSocket(4321, 10, InetAddress.getLocalHost());
            System.out.println("Running Server on " + providerSocket.getLocalSocketAddress());

            for (int i = 0; i < workers; i++) {

                System.out.println("Waiting for connection...");

                Socket connection = providerSocket.accept();

                System.out.println("Got a new connection...");

                out.add(new ObjectOutputStream(connection.getOutputStream()));
                in.add(new ObjectInputStream(connection.getInputStream()));

                // get the Worker's CPU cores & free memory and add them to our list
                int cores = in.get(i).readInt();
                long memory = in.get(i).readLong();

                coresPerWorker.add(cores);
                freeMemPerWorker.add(memory);

                System.out.println("worker " + i + " specs: " + cores + " cores, " + memory + " memory.");

                workerValue.add(coresPerWorker.get(i) * freeMemPerWorker.get(i) * 0.5); // dhmiourgia tou workervalue

            }

            // Uloipoihsh anakatanomhs tou fortou twn workers se periptwsh megalhs kathusterhshs (> 20%) kapoiou worker
            long[] delays = new long[workers];
            double error, delayPercentage;
            double workerPercentage = (double) 1 / workers;   // o enas worker ti pososto twn sunolikwn workers einai
            long delaySum;

            double prevError = 0;
            double threshold = 0.1;

            System.out.println("Every worker must have about the " + (workerPercentage * 100) + "% of the delay.");

            // start our training
            for (int epoch = 0; epoch < epochs; epoch++) {

                //System.out.println("Starting epoch " + epoch + "...");

				sum = 0;
				for (int i = 0; i < workers; i++) {

					sum = sum + workerValue.get(i); //to athroisma olwn twn workervalue olwn twn workers

					/*
					to pososto tou pinaka pou tha parei o kathe worker eksartatai apo to workervalue tou pros to sunoliko workervalue
					o worker i tha parei pososto tou pinaka --> workervalue.get(i) / sum

					* px
					* matrix
					* 1     0 1 6 5 7
					* 2     9 7 4 3 6
					* 3     1 2 0 5 6
					* 4     0 8 7 7 2
					*
					* An o worker 1 exei pososto 80% kai o worker 2 pososto 20%
					* tote o worker 1 tha parei to subMatrix
					* 1     0 1 6 5 7
					* 2     9 7 4 3 6
					* 3     1 2 0 5 6
					*
					* kai o worker 2 to subMatrix
					* 4     0 8 7 7 2
					*/

				}
				
                // we have to reset the starting index at every epoch
                currentStartRowX = 0;
                currentStartRowY = 0;

                for (int i = 0; i < workers; i++) {

                    out.get(i).writeBoolean(true);
                    out.get(i).flush();

                    // distribute X and Y Matrix for each worker
                    distributeXMatrixToWorkers(i, XMatrix);
                    distributeYMatrixToWorkers(i, YMatrix);

                    // send C Matrix
                    out.get(i).writeObject(C);
                    out.get(i).flush();

                    // send P Matrix
                    out.get(i).writeObject(P);
                    out.get(i).flush();

                    // send lambda value
                    out.get(i).writeDouble(lambda);
                    out.get(i).flush();

                }

                int startRowX, startRowY;
                int endRowX, endRowY;

                delaySum = 0;

                // Wait for response
                for (int w = 0; w < workers; w++) {

                    try {

                        //System.out.println("Waiting for worker " + w + "' s response...");

                        // get the calculation delay of worker w
                        delays[w] = in.get(w).readLong();
                        delaySum += delays[w];
                        System.out.println("worker " + w + " delay: " + (delays[w] / 1000000) + "ms");


                        // get X matrix from worker w
                        RealMatrix tempX = (RealMatrix) in.get(w).readObject();
                        //System.out.println("Got X matrix from worker " + w);

                        startRowX = in.get(w).readInt();
                        endRowX = in.get(w).readInt();

                        // update X matrix
                        for (int i = startRowX; i < endRowX; i++) {
                            XMatrix.setRowMatrix(i, tempX.getRowMatrix(i));
                        }


                        // get Y matrix from worker w
                        RealMatrix tempY = (RealMatrix) in.get(w).readObject();
                        //System.out.println("Got Y matrix from worker " + w);

                        startRowY = in.get(w).readInt();
                        endRowY = in.get(w).readInt();

                        // update Y matrix
                        for (int i = startRowY; i < endRowY; i++) {
                            YMatrix.setRowMatrix(i, tempY.getRowMatrix(i));
                        }

                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }

                }

                // Rebalance system according to each worker's delay
                for (int w = 0; w < workers; w++) {

                    delayPercentage = (double) delays[w] / delaySum;
					
					System.out.println("worker " + w + " delays the whole system by " + (delayPercentage * 100) + "%");
//					System.out.println("value of worker " + w + ": " + workerValue.get(w));

                    //                    change the value of the worker w
                    System.out.print("Changed value of worker " + w + " from " + workerValue.get(w));
                    workerValue.set(w, workerValue.get(w) * (1 - (delayPercentage - workerPercentage) / 2));
                    System.out.println(" to " + workerValue.get(w));

                    sum += workerValue.get(w);

                }

                // calculate error
                System.out.println("Calculating error...");

                long startTime;
                long endTime;
                long duration;

                startTime = System.nanoTime();
                error = calculateError();
                endTime = System.nanoTime();

                duration = (endTime - startTime) / 1000000;

                System.out.println("That took me " + duration + "ms to complete.");

                //if (epoch % 10 == 0) System.out.println("Epoch " + epoch + ": " + error);
                System.out.println("Epoch " + (epoch + 1) + ": " + error);

                if (epoch != 0) System.out.println("Error " + ((prevError - error) > 0 ? "de" : "in") + "creased by " + Math.abs(prevError - error));
                if (Math.abs(prevError - error) < threshold) break;

                prevError = error;

            }

            // send signal to stop
            for (int w = 0; w < workers; w++) {
                out.get(w).writeBoolean(false);
                out.get(w).flush();
            }

            // Waiting for client connections
            int currentUser;
            double lat, lon;
            ObjectInputStream in;
            ObjectOutputStream out;

            while (true) {
                System.out.println("Waiting for client connection...");

                Socket connection = providerSocket.accept();

                System.out.println("Got a new connection...");

                in = new ObjectInputStream(connection.getInputStream());
                out = new ObjectOutputStream(connection.getOutputStream());

                currentUser = in.readInt();
                System.out.println("Current user: " + currentUser);

                lat = in.readDouble();
                lon = in.readDouble();
                System.out.println("Current user at: " + lat + ", " + lon);

                out.writeInt(k);
                out.flush();

                out.writeDouble(range);
                out.flush();

                List<Poi> toBeSent = calculateBestLocalPoisForUser(currentUser, lat, lon, k);
                out.writeObject(toBeSent);
                out.flush();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                providerSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }

    }

    public void calculateCMatrix(int a, RealMatrix matrix) {
        for (int i = 0; i < matrix.getRowDimension(); i++) {
            for (int j = 0; j < matrix.getColumnDimension(); j++) {
                C.setEntry(i, j, 1 + a * matrix.getEntry(i, j));
            }
        }
    }

    public void calculatePMatrix(RealMatrix matrix) {
        for (int i = 0; i < matrix.getRowDimension(); i++) {
            for (int j = 0; j < matrix.getColumnDimension(); j++) {
                P.setEntry(i, j, matrix.getEntry(i, j) > 0 ? 1 : 0);
            }
        }
    }

    // compute the XSubMatrix for worker i and send it via out.get(i)
    public void distributeXMatrixToWorkers(int i, RealMatrix matrix) {
        try {
            // send the whole X matrix
            out.get(i).writeObject(XMatrix);
            out.get(i).flush();

            double percent = workerValue.get(i) / sum; // percentage of the X matrix corresponding to the worker i
//            System.out.println("worker " + i + " percentage: " + percent);

            int numberOfRows = (int) Math.round(percent * matrix.getRowDimension());

            //System.out.println("worker " + i + " will take " + numberOfRows + " row" + (numberOfRows == 1 ? "" : "s") + " of the XMatrix.");

            if (numberOfRows < 1) {
                numberOfRows = 1;
            }

            int currentEndRow = i == workers - 1 ? matrix.getRowDimension() - 1 : currentStartRowX + numberOfRows - 1;

            System.out.println("Workload for worker " + i + ": rows " + currentStartRowX + " to " + currentEndRow + " of X matrix.");

//            if (currentStartRowX == matrix.getRowDimension()) currentStartRowX = matrix.getRowDimension() - 1;

            out.get(i).writeInt(currentStartRowX);
            out.get(i).flush();

            out.get(i).writeInt(currentEndRow);
            out.get(i).flush();

            // update the current start row
            currentStartRowX = currentEndRow + 1;

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // compute the YSubMatrix for worker i and send it via out.get(i)
    public void distributeYMatrixToWorkers(int i, RealMatrix matrix) {
        try {
            // send the whole Y matrix
            out.get(i).writeObject(YMatrix);
            out.get(i).flush();

            double percent = workerValue.get(i) / sum; // percentage of the X matrix corresponding to the worker i
//            System.out.println("worker " + i + " percentage: " + percent);

            int numberOfRows = (int) Math.round(percent * matrix.getRowDimension());

            //System.out.println("worker " + i + " will take " + numberOfRows + " row" + (numberOfRows == 1 ? "" : "s") + " of the XMatrix.");

            if (numberOfRows < 1) {
                numberOfRows = 1;
            }

            int currentEndRow = i == workers - 1 ? matrix.getRowDimension() - 1 : currentStartRowY + numberOfRows - 1;

            System.out.println("Workload for worker " + i + ": rows " + currentStartRowY + " to " + currentEndRow + " of Y matrix.");

//            if (currentStartRowY == matrix.getRowDimension()) currentStartRowY = matrix.getRowDimension() - 1;

            out.get(i).writeInt(currentStartRowY);
            out.get(i).flush();

            out.get(i).writeInt(currentEndRow);
            out.get(i).flush();

            // update the current start row
            currentStartRowY = currentEndRow + 1;

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public double calculateError() {

        double minimize_quantity = 0;
        for (int u = 0; u < XMatrix.getRowDimension(); u++) {

            for (int i=0; i < YMatrix.getRowDimension(); i++) {

                double cui = C.getEntry(u, i);
                double pui = P.getEntry(u, i);

                RealMatrix xuT = XMatrix.getRowMatrix(u);
                RealMatrix yi = YMatrix.getRowMatrix(i).transpose();

                minimize_quantity += cui * Math.pow(pui - xuT.multiply(yi).getEntry(0, 0), 2);

            }

        }

        double XuNorm_sum = 0;
        for (int u = 0; u < XMatrix.getRowDimension(); u++) {
            XuNorm_sum += Math.pow(XMatrix.getRowMatrix(u).getFrobeniusNorm(), 2);
        }

        double YiNorm_sum = 0;
        for (int i=0; i < YMatrix.getRowDimension(); i++) {
            YiNorm_sum += Math.pow(YMatrix.getRowMatrix(i).getFrobeniusNorm(), 2);
        }

        double normalize_quantity = lambda * (XuNorm_sum + YiNorm_sum);

        System.out.println("Cost function's first term is: " + minimize_quantity +
                "\nCost function's second term (normalization) is: " + normalize_quantity);

        return minimize_quantity + normalize_quantity;

    }

    public double calculateScore(int u, int i) {
        // xu: f x 1 => xuT: 1 x f
        // u-th row of X matrix: 1 x f (what we want)
        RealMatrix xuT = XMatrix.getRowMatrix(u);

        // yi: f x 1
        // i-th row of Y matrix: 1 x f
        // we want the transpose of the i-th row of Y matrix (f x 1)
        RealMatrix yi = YMatrix.getRowMatrix(i).transpose();

        // rui: 1 x 1
        double rui = xuT.multiply(yi).getEntry(0, 0);

        return rui;
    }

    public List<Poi> calculateBestLocalPoisForUser(int u, double lat, double lon, int k) {
        Map<Double, Poi> map = new HashMap<Double, Poi>();

        ArrayList<Poi> poisInRange = new ArrayList<Poi>();
        for (Poi p : pois) {
            if (calculateDistanceFromLatLon(p.getLatitude(), p.getLongitude(), lat, lon) <= range) {
                System.out.println("found a poi in range.");
                poisInRange.add(p);
            }
        }

        if (poisInRange.isEmpty()) return null;

        double currentScore;
        // check all of the pois
        for (int i = 0; i < poisInRange.size(); i++) {
            // match every poi with its score
            currentScore = calculateScore(u, i);
            System.out.println("score for poi " + i + ": " + currentScore);
            map.put(currentScore, poisInRange.get(i));
        }

        TreeMap<Double, Poi> sortedMap = new TreeMap<Double, Poi>(map);

        ArrayList<Poi> sortedPois = new ArrayList<Poi>(sortedMap.values());
        // we want bigger scores to come first
        Collections.reverse(sortedPois);

        ArrayList<Poi> bestLocalPois = new ArrayList<Poi>();
        // keep the best k pois for user u
        for (int i = 0; i < Math.min(sortedPois.size(), k); i++) {
            bestLocalPois.add(sortedPois.get(i));
        }

        return bestLocalPois;
    }

    private static double calculateDistanceFromLatLon(double lat1, double lon1, double lat2, double lon2) {
        double R = 6371; // Radius of the earth in km
        double dLat = deg2rad(lat2-lat1);  // deg2rad below
        double dLon = deg2rad(lon2-lon1);
        double a = Math.sin(dLat/2) * Math.sin(dLat/2) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLon/2) * Math.sin(dLon/2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        double d = R * c; // Distance in km
        return d;
    }

    private static double deg2rad(double deg) {
        return deg * (Math.PI/180);
    }

    private static String readFile(String filename) {
        String result = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }
            result = sb.toString();
        } catch(Exception e) {
            e.printStackTrace();
        }
        return result;
    }

}

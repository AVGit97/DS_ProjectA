import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

public class WorkerNode extends Thread implements Worker {

    private Socket requestSocket = null;
    private ObjectInputStream in = null;
    private ObjectOutputStream out = null;

    private RealMatrix CuMatrix;
    private RealMatrix CiMatrix;
    private RealMatrix PMatrix;
    private RealMatrix XMatrix;
    private RealMatrix YMatrix;

    private int startRowX, startRowY;
    private int endRowX, endRowY;

    private double lambda;

    private long delay;

    public void initialize() {

        try {

            String hostIP = "192.168.1.2";
            System.out.println("Trying to connect to " + hostIP);

            requestSocket = new Socket(hostIP, 4321);
            System.out.println("Successfully connected to " + hostIP);

            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());

            int cores = Runtime.getRuntime().availableProcessors();
            long memory = Runtime.getRuntime().freeMemory();

            System.out.println("Cores: " + cores + "\tMemory: " + memory);

            out.writeInt(cores);
            out.flush();

            out.writeLong(memory);
            out.flush();

            // while master says true continue
            while (in.readBoolean()) {

                System.out.println("Waiting for workload...");

                try {
                    //wait for workload

//                    for XMatrix
                    XMatrix = (RealMatrix) in.readObject();
                    System.out.println("Got XMatrix from Master.");
                    System.out.println("X: " + XMatrix.getRowDimension() + " x " + XMatrix.getColumnDimension());

                    startRowX = in.readInt();
                    endRowX = in.readInt();
                    System.out.println("My workload for XMatrix is from row " + startRowX + " to row " + endRowX);


//                    for YMatrix
                    YMatrix = (RealMatrix) in.readObject();
                    System.out.println("Got YMatrix from Master.");
                    System.out.println("Y: " + YMatrix.getRowDimension() + " x " + YMatrix.getColumnDimension());

                    startRowY = in.readInt();
                    endRowY = in.readInt();
                    System.out.println("My workload for YMatrix is from row " + startRowY + " to row " + endRowY);


                    // get C matrix
                    RealMatrix CMatrix = (RealMatrix) in.readObject();
                    System.out.println("Got CMatrix from Master.");
//                    System.out.println("C: " + CMatrix.getRowDimension() + " x " + CMatrix.getColumnDimension());

                    // get P matrix
                    PMatrix = (RealMatrix) in.readObject();
                    System.out.println("Got PMatrix from Master.");
//                    System.out.println("P: " + PMatrix.getRowDimension() + " x " + PMatrix.getColumnDimension());

                    // get lambda value
                    lambda = in.readDouble();

                    long startTime = System.nanoTime();

                    // update X matrix
                    System.out.println("Updating X matrix...");
                    for (int u = startRowX; u < endRowX; u++) {
                        // xu: f x 1
                        // xuT: 1 x f
                        XMatrix.setRowMatrix(u, calculate_x_u(u, YMatrix, CMatrix).transpose());
                        //System.out.println("Row " + u + " updated.");
                    }
                    //System.out.println("X matrix updated.");

                    // update Y matrix
                    System.out.println("Updating Y matrix...");
                    for (int i = startRowY; i < endRowY; i++) {
                        // yi: f x 1
                        // yiT: 1 x f
                        YMatrix.setRowMatrix(i, calculate_y_i(i, XMatrix, CMatrix).transpose());
                        //System.out.println("Row " + i + " updated.");
                    }
                    //System.out.println("Y matrix updated.");

                    long endTime = System.nanoTime();

                    delay = endTime - startTime;

                    long duration = (endTime - startTime) / 1000000;

                    System.out.println("That took me " + duration + "ms to complete.");

                    // send delay, X and Y matrix to master
                    System.out.println("Sending results to master...");
                    sendResultsToMaster();

                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

            }

            System.out.println("Done.");

        } catch (UnknownHostException unknownHost) {
            System.err.println("Unknown host!");
        } catch (IOException e) {
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

    public void calculateCMatrix(int i, RealMatrix matrix) {

    }

    // Cu: n x n
    public void calculateCuMatrix(int u, RealMatrix C) {
        // create diagonal matrix
        double[] row = C.getRow(u);
        CuMatrix = MatrixUtils.createRealDiagonalMatrix(row);
    }

    // Ci: m x m
    public void calculateCiMatrix(int i, RealMatrix C) {
        // create diagonal matrix
        double[] column = C.getColumn(i);
        CiMatrix = MatrixUtils.createRealDiagonalMatrix(column);
    }

    public RealMatrix preCalculateYY(RealMatrix matrix) {
        return matrix.multiply(matrix.transpose());
    }

    public RealMatrix preCalculateXX(RealMatrix matrix) {
        return matrix.multiply(matrix.transpose());
    }

    public RealMatrix calculate_x_u(int u, RealMatrix Y, RealMatrix C) {
        calculateCuMatrix(u, C);

        RealMatrix YT = Y.transpose();

        // I: f x f
        RealMatrix I = MatrixUtils.createRealIdentityMatrix(Y.getColumnDimension());

        // lambda * I
        RealMatrix lambda_I = I.scalarMultiply(lambda);

        // xu = Y^T * Cu * Y + lambda * I
        RealMatrix x_u = YT.multiply(CuMatrix).multiply(Y).add(lambda_I);

        // xu = xu^(-1)
        RealMatrix inverted = new LUDecomposition(x_u).getSolver().getInverse();

        // pu = Pu^T
        RealMatrix p_u = MatrixUtils.createColumnRealMatrix(PMatrix.getRow(u));

        RealMatrix inverted_YT = inverted.multiply(YT);

        RealMatrix Cu_pu = CuMatrix.multiply(p_u);

        // xu = inverted * Y^T * Cu * pu
        x_u = inverted_YT.multiply(Cu_pu);

        // xu: f x 1
        return x_u;
    }

    public RealMatrix calculate_y_i(int i, RealMatrix X, RealMatrix C) {
        calculateCiMatrix(i, C);

        RealMatrix XT = X.transpose();

        // I: f x f
        RealMatrix I = MatrixUtils.createRealIdentityMatrix(X.getColumnDimension());

        // lambda * I
        RealMatrix lambda_I = I.scalarMultiply(lambda);

        // yi = X^T * X * Ci + lambda * I
        RealMatrix y_i = XT.multiply(CiMatrix).multiply(X).add(lambda_I);

        // yi = yi^(-1)
        RealMatrix inverted = new LUDecomposition(y_i).getSolver().getInverse();

        // pi = Pi
        RealMatrix p_i = MatrixUtils.createColumnRealMatrix(PMatrix.getColumn(i));

        RealMatrix inverted_XT = inverted.multiply(XT);

        RealMatrix Ci_pi = CiMatrix.multiply(p_i);

        // yi = inverted * X^T * Ci * pi
        y_i = inverted_XT.multiply(Ci_pi);

        // yi: f x 1
        return y_i;
    }

    public void sendResultsToMaster() {

        try {
            out.writeLong(delay);
            out.flush();

//            send back the X matrix
            out.writeObject(XMatrix);
            out.flush();

            out.writeInt(startRowX);
            out.flush();

            out.writeInt(endRowX);
            out.flush();


//            send back the Y matrix
            out.writeObject(YMatrix);
            out.flush();

            out.writeInt(startRowY);
            out.flush();

            out.writeInt(endRowY);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void run() {
        initialize();
    }

}

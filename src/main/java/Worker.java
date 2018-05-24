import org.apache.commons.math3.linear.RealMatrix;

public interface Worker {

    void initialize();

    void calculateCMatrix(int i, RealMatrix matrix);

    void calculateCuMatrix(int i, RealMatrix matrix);

    void calculateCiMatrix(int i, RealMatrix matrix);

    RealMatrix preCalculateYY(RealMatrix matrix);

    RealMatrix preCalculateXX(RealMatrix matrix);

    RealMatrix calculate_x_u(int i, RealMatrix matrix, RealMatrix matrix2);

    RealMatrix calculate_y_i(int i, RealMatrix matrix, RealMatrix matrix2);

    void sendResultsToMaster();

}

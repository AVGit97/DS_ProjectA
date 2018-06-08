import org.apache.commons.math3.linear.RealMatrix;
import test.ds_project.poi.Poi;

import java.util.List;

public interface Master {

    void initialize();

    void calculateCMatrix(int i, RealMatrix matrix);

    void calculatePMatrix(RealMatrix matrix);

    void distributeXMatrixToWorkers(int i, RealMatrix matrix);

    void distributeYMatrixToWorkers(int i, RealMatrix matrix);

    double calculateError();

    double calculateScore(int i, int j);

    List<Poi> calculateBestLocalPoisForUser(int i, double x, double y, int j);

}

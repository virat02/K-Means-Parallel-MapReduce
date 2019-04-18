package seqkmeans;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class SeqKMeans {

    // Manhattan distance
    private Double distance(ArrayList<Double> centroid, ArrayList<Double> record)
    {
        double sum = 0.0;
        int size = centroid.size();
        // ignoring the last element ... which is the actual label for now
        for (int i = 0; i < size - 1; i++) {
            sum += Math.abs(centroid.get(i) - record.get(i));
        }
        return sum;
    }

    public static void main(String[] args) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader("input/winequality-red.csv"));
        String label_line = br.readLine();
        String[] recordValuesAsString = label_line.split(";");
        ArrayList<Double> record = new ArrayList<>();

        for (String s : recordValuesAsString) {
            record.add(Double.parseDouble(s));
        }

    }
}

package seqkmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class SeqKMeans {

    private static ArrayList<ArrayList<Double>> records = new ArrayList<>();
    private static ArrayList<ArrayList<Double>> centroids = new ArrayList<>();
    private static int k = 2;
    private static HashMap<ArrayList<Double>, Integer> result = new HashMap<>();

    // Manhattan distance
    private static Double distance(ArrayList<Double> centroid, ArrayList<Double> record)
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

        // Building our dataset in memory
        BufferedReader br = new BufferedReader(new FileReader("input/winequality-red.csv"));
        String line;
        while ((line = br.readLine()) != null){
            String[] recordValuesAsString = line.split(";");
            ArrayList<Double> record = new ArrayList<>();
            for (String s : recordValuesAsString) {
                record.add(Double.parseDouble(s));
            }
            records.add(record);
        }
        br.close();

        // Selecting k centroids at random
        // Reference - https://stackoverflow.com/questions/12487592/randomly-select-an-item-from-a-list
        int i = 0;
        while (i < k){
            Random random = new Random();
            ArrayList<Double> centroid = records.get(random.nextInt(records.size()));
            centroids.add(centroid);
            i++;
        }

        // creating k entries in hashmap, one for each centroid
        i = 1;
        for (ArrayList<Double> c: centroids) {
            result.put(c, i);
            i++;
        }

        System.out.println(result);

        // running k means
        for (ArrayList<Double> r: records) {
            Double min_val = Double.MAX_VALUE;
            ArrayList<Double> selected_centroid;
            for (ArrayList<Double> c: centroids) {
                Double d = distance(c, r);
            }
        }
    }
}

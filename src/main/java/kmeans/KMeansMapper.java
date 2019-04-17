package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {

    List<ArrayList<Double>> centroids = new ArrayList<>();
    int K = 0;

    @Override
    protected void setup(Mapper<Object, Text, IntWritable, Text>.Context context)
            throws IOException, InterruptedException {
        super.setup(context);

        URI[] files = context.getCacheFiles();
        K = context.getConfiguration().getInt("K", 0);

        for (int i = 0; i < K; i++) {
            centroids.add(new ArrayList<Double>());
        }

        for (URI f : files) {
            FileSystem fs = FileSystem.get(f, context.getConfiguration());

            Path path = new Path(f.toString());
            BufferedReader br = new BufferedReader(new InputStreamReader((fs.open(path))));

            String line;
            while ((line = br.readLine()) != null) {
                String[] keyvalue = line.split(":");
                int index = Integer.parseInt(keyvalue[0]);
                String[] temp = keyvalue[1].split(";");

                // bad initial cluster selection we have a cluster center with no points
                if (temp.length == 0){
                    continue;
                }

                ArrayList<Double> record = centroids.get(index);
                for (String t : temp) {
                    record.add(Double.parseDouble(t));
                }
            }
            br.close();
        }
    }

    // Euclidian distance
    // private Double distance(ArrayList<Double> centroid, ArrayList<Double> record)
    // {
    // double sum = 0.0;
    // int size = centroid.size();
    // // ignoring the last elememt ... which is the actual label for now
    // for (int i = 0; i < size - 1; i++) {
    // sum += Math.pow(centroid.get(i) - record.get(i), 2);
    // }
    // return Math.sqrt(sum);
    // }

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

    // Minkowski distance
    // private Double distance(ArrayList<Double> centroid, ArrayList<Double> record) {
    //     double sum = 0.0;
    //     // => manhattan distance
    //     double q = 3;
    //     int size = centroid.size();
    //     // ignoring the last element ... which is the actual label for now
    //     for (int i = 0; i < size - 1; i++) {
    //         //considering only 0-alcohol, 6-flavinoids
    //         if(!(i == 0 || i == 6)){
    //             continue;
    //         }
    //         sum += Math.pow(centroid.get(i) - record.get(i),q);
    //     }
    //     return Math.pow(sum, 1.0 / q);
    // }

    @Override
    public void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        Double min = null;
        Integer minCentroidKey = null;
        int size = centroids.size();
        double dist = 0;
        ArrayList<Double> record = new ArrayList<Double>();
        String[] temp = value.toString().split(";");
        for (String t : temp) {
            record.add(Double.parseDouble(t));
        }

        for (int i = 0; i < size; i++) {
            dist = distance(centroids.get(i), record);
            if (min == null || min > dist) {
                min = dist;
                minCentroidKey = i;
            }
        }

        context.write(new IntWritable(minCentroidKey), value);
    }

    @Override
    protected void cleanup(Mapper<Object, Text, IntWritable, Text>.Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
        for (int i = 0; i < K; i++) {
            context.write(new IntWritable(i), new Text("NULL"));
        }
    }
}
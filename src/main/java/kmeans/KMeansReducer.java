package kmeans;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable arg0, Iterable<Text> arg1,
            Reducer<IntWritable, Text, IntWritable, Text>.Context arg2) throws IOException, InterruptedException {
        ArrayList<Double> centroid = new ArrayList<Double>();
        ArrayList<ArrayList<Double>> records = new ArrayList<>();
        
        for (Text t : arg1) {
            ArrayList<Double> record = new ArrayList<Double>();
            for (String i : t.toString().split(";")) {
                record.add(Double.parseDouble(i));
            }
            records.add(record);
        }

        int size = records.get(0).size();
        for (int i = 0; i < size; i++) {
            centroid.add(0.0);
        }

        for (ArrayList<Double> r : records) {
            for (int i = 0; i < size; i++) {
                centroid.set(i, centroid.get(i) + r.get(i));
            }
        }

        String[] value = new String[size];
        for (int i = 0; i < size; i++) {
            value[i] = Double.toString(centroid.get(i) / records.size());
        }

        arg2.write(arg0, new Text(String.join(";", value)));
    }
}
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
        int size = 0;
        int numFields = 0;
        boolean first = true;
        int ctr = 0;

        for (Text t : arg1) {
            size++;
            ctr = 0;
            for (String i : t.toString().split(";")) {
                if (first) {
                    numFields++;
                    centroid.add(0.0);
                }
                centroid.set(ctr,centroid.get(ctr) + Double.parseDouble(i));
                ctr++;
            }
            first = false;
        }


        ArrayList<String> value = new ArrayList<String>();
        for (int i = 0; i < numFields; i++) {
            value.add(Double.toString(centroid.get(i) / size));
        }

        arg2.write(arg0, new Text(String.join(";",value)));
    }
}
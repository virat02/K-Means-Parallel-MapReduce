package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    private MultipleOutputs<IntWritable,Text> mos;
    private String [] centroids;

    private void getDataFromCache(Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException {
        int K = context.getConfiguration().getInt("K", 0);
        URI[] files = context.getCacheFiles();
        centroids = new String[K];

        for (URI f : files) {
            FileSystem fs = FileSystem.get(f, context.getConfiguration());

            Path path = new Path(f.toString());
            BufferedReader br = new BufferedReader(new InputStreamReader((fs.open(path))));

            String line;
            while ((line = br.readLine()) != null) {
                String[] keyvalue = line.split(":");
                int index = Integer.parseInt(keyvalue[0]);
                centroids[index] = keyvalue[1];
            }
            br.close();
        }
    }

    @Override
    protected void setup(Reducer<IntWritable, Text, IntWritable, Text>.Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<>(context);
        getDataFromCache(context);

    }
    @Override
    protected void reduce(IntWritable arg0, Iterable<Text> arg1,
            Reducer<IntWritable, Text, IntWritable, Text>.Context arg2) throws IOException, InterruptedException {
        ArrayList<Double> centroid = new ArrayList<>();
        int size = 0;
        int numFields = 0;
        boolean first = true;
        int ctr;

        for (Text t : arg1) {

            if(t.toString().equals("NULL")){
                continue;
            }

            size++;
            ctr = 0;
            mos.write("clusters", arg0, t);
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

        if(size > 0){

            ArrayList<String> value = new ArrayList<>();
            for (int i = 0; i < numFields; i++) {
                value.add(Double.toString(centroid.get(i) / size));
            }

            int counter = 0;
            double res;
            boolean setcounter = false;
            
            for(String i: centroids[arg0.get()].split(";")){
                res = Double.parseDouble(i) - Double.parseDouble(value.get(counter++));
                if(res > 0.1){
                    setcounter = true;
                }
            }

            if(setcounter){
                arg2.getCounter(KMeans.counter.terminate).increment(1);
            }

            arg2.write(arg0, new Text(String.join(";",value)));
        }  else {
            // arg2.write(arg0, new Text(String.join(";",centroids[arg0.get()])));
            arg2.write(arg0, new Text(""));
        }       
    }

    @Override
    protected void cleanup(Reducer<IntWritable, Text, IntWritable, Text>.Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
        mos.close();
    }
}
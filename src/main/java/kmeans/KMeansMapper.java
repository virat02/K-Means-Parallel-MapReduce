package  kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<Object, Text, Text, Text> {

    List<Double> centroids = new ArrayList<>();

    @Override
    protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);

        URI[] files = context.getCacheFiles();

        for (URI f : files){
            FileSystem fs = FileSystem.get(f, context.getConfiguration());

            Path path = new Path(f.toString());
            BufferedReader br = new BufferedReader(new InputStreamReader((fs.open(path))));

            String line;

            while ((line = br.readLine()) != null){
                centroids.add(Double.parseDouble(line));
            }
            br.close();
        }
    }

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

    }
}
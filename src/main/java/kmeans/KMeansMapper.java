package  kmeans;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        
    }

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

    }
}
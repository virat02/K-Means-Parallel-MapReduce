package kmeans;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Main class that executes the K Means Algorithm
 */
public class KMeans extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(KMeans.class);

    public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        return 0;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new KMeans(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
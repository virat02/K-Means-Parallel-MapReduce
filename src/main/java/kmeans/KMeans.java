package kmeans;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Main class that executes the K Means Algorithm
 */
public class KMeans extends Configured implements Tool {

    public static enum counter {
        terminate
    };

    private static final Logger logger = LogManager.getLogger(KMeans.class);

    @Override
    public int run(final String[] args) throws Exception {

        int countr = 0;
        boolean terminate = false;

        //select initial K centroids
        // Configuration conf1 = getConf();
        // conf1.set("K", args[2]);
        // Job job1 = Job.getInstance(conf1,"InitialCentroids");
        // job1.setJarByClass(KMeans.class);
        // job1.setMapperClass(KMeansMapper.class);
        // job1.setOutputKeyClass(Text.class);
        // job1.setOutputValueClass(Text.class);
        // FileInputFormat.addInputPath(job1, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job1, new Path("centroids" + counter));
        // return job1.waitForCompletion(true)?0:1;

        String  K  = args[2];
        while (true) {
            Configuration conf = getConf();
            conf.set("mapreduce.output.textoutputformat.separator", ":");
            conf.set("K", K);
            Job job = Job.getInstance(conf, "clustering");
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));

            // broadcast centroids for next iteration
            FileSystem fs = FileSystem.get(conf);
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("centroids" + countr), false);
            while (files.hasNext()) {
                Path filePath = files.next().getPath();
                String[] temp = filePath.toString().split("/");
                if (temp[temp.length - 1].contains("part")) {
                    logger.info("ADDING FILES TO CACHE : " + filePath.toString());
                    job.addCacheFile(filePath.toUri());
                }
            }

            countr++;
            if(terminate){
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
            } else {
                FileOutputFormat.setOutputPath(job, new Path("centroids" + countr));
            }
            
            MultipleOutputs.addNamedOutput(job, "clusters", TextOutputFormat.class, IntWritable.class, Text.class);

            if(!job.waitForCompletion(true)){
                return 1;
            }

            if(terminate){
                break;
            }

            if (job.getCounters().findCounter(counter.terminate).getValue() == 0) {
                terminate = true;
            }
        }

        return 0;
    }

    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Three arguments required:\n<input-dir> <output-dir> <K>");
        }

        try {
            ToolRunner.run(new KMeans(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
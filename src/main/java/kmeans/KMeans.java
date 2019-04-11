package kmeans;

import java.io.IOException;

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

/**
 * Main class that executes the K Means Algorithm
 */
public class KMeans extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(KMeans.class);

    @Override
    public int run(final String[] args) throws Exception {
        // TODO select initial K centroids
        // Configuration conf = getConf();
        // conf.set("K", args[2]);
        // Job job = Job.getInstance(conf,"InitialCentroids");
        // job.setJarByClass(KMeans.class); 
        // job.setMapperClass(KMeansMapper.class);   
        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(Text.class); 
        // FileInputFormat.addInputPath(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // return job.waitForCompletion(true)? 0 : 1;

        int counter = 0;
        int ret;
        while(true){
            Configuration conf = getConf();
            conf.set("mapreduce.output.textoutputformat.separator", ":");
            conf.set("K", args[2]);
            Job job = Job.getInstance(conf,"clustering");
            job.setJarByClass(KMeans.class); 
            job.setMapperClass(KMeansMapper.class); 
            job.setReducerClass(KMeansReducer.class);  
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));

            // broadcast centroids for next iteration 
            FileSystem fs = FileSystem.get(conf);
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("centroids" + counter), false);
            while(files.hasNext()){
                Path filePath = files.next().getPath();
                logger.info("ADDING FILES TO CACHE : "+ filePath.toString());
                job.addCacheFile(filePath.toUri());
            }
            
            counter++;
            FileOutputFormat.setOutputPath(job, new Path("centroids" + counter));
            ret =  job.waitForCompletion(true)? 0 : 1;

            //TODO termination condition 
            if(counter == 2){
                break;
            }
        }
        return ret;
    }

    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir> <K>");
        }

        try {
            ToolRunner.run(new KMeans(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
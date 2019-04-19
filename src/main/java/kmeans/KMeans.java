package kmeans;

import org.apache.hadoop.mapreduce.Job;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
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
        String fileName;
        String K = args[3];
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

            if (countr == 0) {
                job.addCacheFile((new Path(args[2])).toUri());
            } else {
                // broadcast centroids for next iteration
                fileName = "centroids" + countr;
                // https://stackoverflow.com/questions/11342400/how-to-list-all-files-in-a-directory-and-its-subdirectories-in-hadoop-hdfs
                FileSystem fs = FileSystem.get(new URI(fileName + "/"), conf);
                FileStatus[] fileStatus = fs.listStatus(new Path(fileName + "/"));
                for (FileStatus status : fileStatus) {
                    String filePath = status.getPath().toString();
                    String[] temp = filePath.split("/");
                    if (temp[temp.length - 1].contains("part")) {
                        logger.info("ADDING FILES TO CACHE : " + filePath);
                        job.addCacheFile(new URI(filePath));
                    }
                }
            }

            countr++;
            if (terminate) {
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
            } else {
                FileOutputFormat.setOutputPath(job, new Path("centroids" + countr));
            }

            MultipleOutputs.addNamedOutput(job, "clusters", TextOutputFormat.class, IntWritable.class, Text.class);

            if (!job.waitForCompletion(true)) {
                return 1;
            }

            if (terminate) {
                break;
            }

            if (job.getCounters().findCounter(counter.terminate).getValue() == 0) {
                terminate = true;
            }
        }

        return 0;
    }

    public static void main(final String[] args) {
        if (args.length != 4) {
            throw new Error("Four arguments required:\n<input-dir> <output-dir> <centroids0> <K>");
        }

        try {
            ToolRunner.run(new KMeans(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
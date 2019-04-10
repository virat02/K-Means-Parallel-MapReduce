package kmeans;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public  class KMeansReducer extends Reducer<Text, Text, Text, Text> {
   @Override
   protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
           throws IOException, InterruptedException {
       super.reduce(arg0, arg1, arg2);
   }
}
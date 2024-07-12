import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class ProcessUnits {
   // Map function
   public static class E_EMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();

      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
         String line = value.toString();
         String[] tokens = line.split(",");

         if (tokens.length >= 2) {
            String date = tokens[0].trim();
            String lastToken = tokens[tokens.length - 1].trim();

            try {
               int stockPrice = Integer.parseInt(lastToken);

               // Extract month from the date (assuming date format: "yyyy-MM-dd")
               String month = date.substring(0,4);

               output.collect(new Text(month), new IntWritable(stockPrice));
            } catch (NumberFormatException e) {
               // Handle invalid number format
            }
         }
      }
   }

   // Reduce function
   public static class E_EReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
      public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
            Reporter reporter) throws IOException {
         int maxStockPrice = Integer.MIN_VALUE;

         while (values.hasNext()) {
            int currentStockPrice = values.next().get();
            maxStockPrice = Math.max(maxStockPrice, currentStockPrice);
         }

         output.collect(key, new IntWritable(maxStockPrice));
      }
   }

   // Main function
   public static void main(String args[]) throws Exception {
      JobConf conf = new JobConf(ProcessUnits.class);
      conf.setJobName("max_stockprice_monthly");

      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(IntWritable.class);

      conf.setMapperClass(E_EMapper.class);
      conf.setCombinerClass(E_EReduce.class);
      conf.setReducerClass(E_EReduce.class);

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));

      JobClient.runJob(conf);
   }
}

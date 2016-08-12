package hadoop.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hsqldb.lib.HashMap;

public class WordCount1 {

  public static class TokenizerMapper1
       extends Mapper<Object, Text, Text, Text>{


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    
      String[] input = value.toString().split(".txt"); 
      String wordFile = input[0]+".txt";
      String[] wordfile= wordFile.split("&");
      String count = input[1].trim();
      String fileName = wordfile[1];
      String wordCount = wordfile[0]+"&"+count;
      context.write(new Text(fileName),new Text(wordCount));	  
    }
  }

  public static class IntSumReducer1
       extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0; 
      String result = "" ;
      for (Text val : values) {
    	 result = result + val + ",";
    	 String[] wordCount = val.toString().split("&"); 
         String count = wordCount[1]; 
         int count1 = Integer.parseInt(count);
    	 sum += count1;
      }
      String[] resultWords = result.split(",");
      List<String> stop = Arrays.asList(resultWords);
      for (String val1 : stop) {
      	 String[] wordCount = val1.toString().split("&"); 
      	 String countSum = wordCount[1]+"&"+sum; 
      	 String wordFile = wordCount[0]+ "&" + key.toString();
      	 context.write(new Text(wordFile), new Text(countSum));
        }
    }
  }

  public static void main(String[] args) throws Exception {
	JobConf conf= new JobConf(WordCount1.class);
    Job job = new Job(conf);
    job.setJarByClass(WordCount1.class);
    job.setMapperClass(TokenizerMapper1.class);
    job.setReducerClass(IntSumReducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

package hadoop.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount3 {

  public static class TokenizerMapper3
       extends Mapper<Object, Text, Text, FloatWritable>{
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	  String[] input = value.toString().split(".txt"); 
          String wordFile = input[0] + ".txt";
          String countSumM= input[1].trim();
          String[] countsumM = countSumM.toString().split("&");
          String count=countsumM[0];
          double n = Double.parseDouble(count);
          String sum = countsumM[1];
          double N= Double.parseDouble(sum);
          String m1 = countsumM[2];
          double m= Double.parseDouble(m1);
          double D=10;
    	
    	  float TF_IDF ;
    	  TF_IDF = (float) ((float) (n/N) * (Math.log(D/m)));
    	  
    	   
    	  FloatWritable tf= new FloatWritable();
    	  tf.set(TF_IDF);
    	 context.write( new Text(wordFile) ,tf );	  
    	
      
    }
  }

  public static class IntSumReducer3
       extends Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount3.class);
    job.setMapperClass(TokenizerMapper3.class);
  //  job.setCombinerClass(IntSumReducer.class);
   // job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);    // Specifying 0 Reducers
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

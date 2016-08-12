package hadoop.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hsqldb.lib.HashMap;

public class WordCount2 {

  public static class TokenizerMapper2
       extends Mapper<Object, Text, Text, Text>{
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

        String[] input = value.toString().split(".txt"); 
        String wordFile = input[0]+".txt";
        String[] wordfile= wordFile.split("&");
        String sumCount = input[1].trim();
        String word = wordfile[0];
        String fileSumCountOne = wordfile[1]+"&"+sumCount+ "&"+"1";
      	context.write(new Text(word),new Text(fileSumCountOne));	
          
    }
  }

  public static class IntSumReducer2
       extends Reducer<Text,Text,Text,Text> {
 

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
        	int sum = 0;     
           String result = "" ;
           for (Text val : values) {	
         	   result = result + val + ",";
         	   String[] input = val.toString().split("&"); 
         	   String one = input[3]; 
         	    int ones = Integer.parseInt(one);
         	    sum += ones;
          //sum++;
         }
       
           String[] resultWords = result.split(",");
           List<String> stop = Arrays.asList(resultWords);
           for (String fileSumCountOne : stop) {
           	 String[] filesumcountone = fileSumCountOne.toString().split("&"); 
           	 String sumCountm =  filesumcountone[1]+ "&" +  filesumcountone[2]+ "&" +sum; 
           	 String wordFile = key.toString() + "&" + filesumcountone[0];
           	 context.write(new Text(wordFile), new Text(sumCountm));
           }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount2.class);
    job.setMapperClass(TokenizerMapper2.class);
   // job.setCombinerClass(IntSumReducer2.class);
    job.setReducerClass(IntSumReducer2.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

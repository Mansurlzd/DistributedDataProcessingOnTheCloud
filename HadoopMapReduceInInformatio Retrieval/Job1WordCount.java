package ee.ut.cs.ddpc; /**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Job1WordCount {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {


      String itr[]=value.toString().toLowerCase().replaceAll("[^a-zA-Z ]", "").split("\\s+");
      /*we can remove punctuations (numbers and digits also) with replaceAll("\\p{P}", "") this method as well. */
      String[] fileNames = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().toString().split("/");
      String filename=fileNames[(fileNames.length-1)];
    	//TODO: Split the line into words and output each word.
      for(String w : itr){
        if(w.length()>0){
          context.write(new Text(w+";"+filename), new IntWritable(1));
        }
      }

    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  
	  
	  
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	
    	//TODO: Sum all the counts as n
      int sum=0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    	
    }
    
    
  }

  public static void main(String[] args) throws Exception {
      org.apache.log4j.BasicConfigurator.configure();
      org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.INFO);
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    
    
    
   
    (new Path(otherArgs[1])).getFileSystem(conf).delete(new Path(otherArgs[1]));
    
    job.setJarByClass(Job1WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    
    job.setCombinerClass(IntSumReducer.class);
    
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

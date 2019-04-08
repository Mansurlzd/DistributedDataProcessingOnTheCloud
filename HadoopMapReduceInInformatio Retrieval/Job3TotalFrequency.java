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
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Job3TotalFrequency {

  public static class TokenizerMapper
       extends Mapper<Text, Text, Text, Text>{


    public void map(Text key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String[] full=key.toString().split(";");
      context.write(new Text(full[0]), new Text(full[1] + ";" + value + ";" + 1));
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {


    public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {



      ArrayList<String[]> listOfValues = new ArrayList();
      double m = 0.0D;
      double D = Double.parseDouble(context.getConfiguration().get("D"));
      for (Text val : values) {
        listOfValues.add(val.toString().split(";"));
        m += 1.0D;
      }
      for (String[] vals : listOfValues) {
        String filename = vals[0];
        double n = Double.valueOf(vals[1]).doubleValue();
        double N = Double.valueOf(vals[2]).doubleValue();
        double tfidf = n / N * Math.log(D / m);
        context.write(new Text(key + ";" + filename), new Text(" " + tfidf));
      }

    }
  }

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.INFO);
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    conf.set("D", otherArgs[2]);
    if (otherArgs.length != 3) {
      System.err.println("Usage: wordcount <in> <out> <D>");
      System.exit(2);
    }

    (new Path(otherArgs[1])).getFileSystem(conf).delete(new Path(otherArgs[1]));


    Job job = new Job(conf, "Word Frequency");
    job.setJarByClass(Job3TotalFrequency.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);


    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

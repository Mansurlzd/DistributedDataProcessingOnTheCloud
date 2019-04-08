/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ee.ut.cs.ddpc;

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

public class Lab3MRapp {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static Text outvalue = new Text();
        private Text outkey = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] values = value.toString().split(",");
            String bal = values[2];
            String city = values[4];
            String year=context.getConfiguration().get("year");
            String date=values[5];
            if(values.length==7)
                if(date.contains(year))
                    context.write(new Text(city), new Text(bal));


        }
    }




    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            double count = 0;
            double max=0;
            double first_h=0;
            double second_h=0;
            double third_h= 0;
            double first_l=0;
            double second_l=0;
            double third_l=0;
            double min=99999;
            double num = 0;
            for (Text val : values) {

                    num = Double.parseDouble(val.toString());
                    sum += Double.parseDouble(val.toString());
               /*     if(max<num){
                        max=num;
                    }
                    if(min>num){
                    min=num;
                    } */
                    if (num > first_h)
                    {
                        third_h=second_h;
                        second_h=first_h;
                        first_h = num;
                    }
                   else if(num>second_h)
                    {
                        third_h=second_h;
                        second_h=num;
                    }
                    else if(num>third_h)
                    {
                        third_h=num;
                    }
                    if(num<first_l)
                    {
                        third_l=second_l;
                        second_l=first_l;
                        first_l = num;
                    }
                    else if(num>second_l)
                    {
                        third_l=second_l;
                        second_l=num;
                    }
                    else if(num>third_l)
                    {
                        third_l=num;
                    }
                    count++;

            }
            double avg = sum / count;
            context.write(key, new Text("Avg:" + avg+" First high:"+first_h+" Second high: "+second_h+" Third high: "+third_h));
            context.write(key, new Text("Avg:" + avg+" First low:"+first_l+" Second low : "+second_l+" Third low: "+third_l));
//            context.write(key, new Text("max:" + max));
//             context.write(key, new Text("min:" + min));
        }
    }

    public static void main(String[] args) throws Exception {


        org.apache.log4j.BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.INFO);
        Configuration conf = new Configuration();



        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        String year =otherArgs[2];
        conf.set("year",year);

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Lab3MRapp.class);
        job.setMapperClass(Lab3MRapp.TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

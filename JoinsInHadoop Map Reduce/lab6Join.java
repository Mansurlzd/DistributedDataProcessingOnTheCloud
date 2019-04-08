package ee.ut.cs.ddpc;

/**
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Lab6Join {

    public static class Lab6JoinMapper
            extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] data = value.toString().split(",");
            String store = data[0];
            if (store.equals("Store")) {
                return;
            }

            if (data.length == 5) {
                context.write(new Text(store + ";" + data[2]), new Text("ws"  + ";" + data[3]));
            }
            else if (data.length == 12) {

                context.write(new Text(store + ";" + data[1]), new Text("fp"+ ";" + data[3]));
            }

        }
    }

    public static class Lab6joinReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            double fuel_price = 0.0D;
            double sum = 0.0D;
            int count = 0;

            for (Text value : values) {
                String[] datas = value.toString().split(";");
                if (datas[0].equals("ws")) {
                    double weekly_sales = Double.parseDouble(datas[1]);
                    if (weekly_sales > max) max = weekly_sales;
                    if (weekly_sales < min) min = weekly_sales;
                    sum += weekly_sales;
                    count++;
                } else if (datas[0].equals("fp")) {
                    fuel_price = Double.parseDouble(datas[1]);
                }
            }
            double average;
            average= sum / count;
            if (fuel_price > 3.0D) {
                context.write(key, new Text("AVG: " + average+" MIN "+min+" MAX "+max));

            }
        }
    }

    public static void main(String[] args) throws Exception {
        //configure logging
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(org.apache.log4j.Level.INFO);

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Lab6Join <in> <out> ");
            System.exit(2);
        }

        String input_path = otherArgs[0];
        String output_path = otherArgs[1];

        //NB! Be careful, output folder(s) will be deleted automatically
        (new Path(otherArgs[1])).getFileSystem(conf).delete(new Path(output_path));


        //configure the job
        Job job = new Job(conf, "Lab6 example Join Skeleton" );

        job.setJarByClass(Lab6Join.class);
        job.setMapperClass(Lab6JoinMapper.class);
        job.setReducerClass(Lab6joinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));

        //Wait until job finishes.
        job.waitForCompletion(true);
        System.exit(0);
    }
}

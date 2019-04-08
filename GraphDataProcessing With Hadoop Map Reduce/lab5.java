package ee.ut.cs.ddpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

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




public class Lab5Depth
{
    public Lab5Depth() {}

    public static class Lab5DepthMapper
            extends Mapper<Text, Text, Text, Text>
    {
        public Lab5DepthMapper() {}

        public void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException
        {
            String startNode = context.getConfiguration().get("startNode");
            String[] values = value.toString().split(";");
            String neighbours;
            String[] adjancency_list;
            int D;

            if (!value.toString().contains(";")) {
                if (key.toString().equals(startNode))
                    D=0;
                else{
                    D=-1;
                }
                neighbours = value.toString();
            } else {
                D=Integer.parseInt(values[0]);
                neighbours=values.length==1 ? "" : values[1];

            }
            if (D != -1) {

                if (values.length > 1) {
                    adjancency_list = values[1].split(" "); }
                else {
                    if (value.toString().contains(";")) {
                        adjancency_list = new String[0];
                    } else {
                        adjancency_list = value.toString().split(" ");
                    }
                }
                for (String neighbor : adjancency_list) {
                    if (neighbor.length() > 0) {

                        context.write(new Text(neighbor), new Text("DISTANCE" +";" + (D+1)));
                    }
                }
            }
            context.write(key, new Text(D + ";" + neighbours));
        }
    }

    public static class Lab5DepthReducer
            extends Reducer<Text, Text, Text, Text>
    {
        public Lab5DepthReducer() {}

        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException
        {
            int distance = -1;
            String adj_list = "";
            ArrayList<Integer> distances = new ArrayList();
            int min_distance;

            for (Text value : values)
            {
                String[] val = value.toString().split(";");
                if (value.toString().contains("DISTANCE")) {
                    distances.add(Integer.valueOf(Integer.parseInt(val[1])));
                } else {
                    distance = Integer.parseInt(val[0]);
                    adj_list = val.length > 1 ? val[1] : "";
                }
            }
            if(distances.size()>0){
                min_distance=Collections.min(distances);
            }
            else {
                min_distance=distance;
            }

            context.write(key, new Text(min_distance+";"+adj_list));
        }
    }


    public static void main(String[] args)
            throws Exception
    {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: Lab5Depth <in> <out> start_node_id depth");
            System.exit(2);
        }

        String input_path = otherArgs[0];
        String output_path = otherArgs[1];
        String initial_node = otherArgs[2];
        int depth = Integer.parseInt(otherArgs[3]);
        conf.set("startNode", initial_node);
        conf.set("N", otherArgs[3]);

        new Path(otherArgs[1]).getFileSystem(conf).delete(new Path(output_path), true);

        for (int i = 0; i < depth; i++)
        {
            String job_output_path = output_path + "/out_" + i;

            Job job = Job.getInstance(conf, "Lab5 graph processing job nr:" + i);

            job.setJarByClass(Lab5Depth.class);
            job.setMapperClass(Lab5DepthMapper.class);
            job.setReducerClass(Lab5DepthReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);

            FileInputFormat.addInputPath(job, new Path(input_path));
            FileOutputFormat.setOutputPath(job, new Path(job_output_path));


            job.waitForCompletion(true);
            input_path = job_output_path;
        }
        System.exit(0);
    }
}

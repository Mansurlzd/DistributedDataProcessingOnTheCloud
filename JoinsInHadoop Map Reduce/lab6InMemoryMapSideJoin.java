package ee.ut.cs.ddpc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;





public class Lab6InMemoryMapSideJoin
{
    public Lab6InMemoryMapSideJoin() {}

    public static class Lab6InMemoryMapSideJoinMapper
            extends Mapper<Object, Text, Text, Text>
    {
        public Lab6InMemoryMapSideJoinMapper() {}

        HashMap<String, Double> features = new HashMap();

        public void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException {
            if ((context.getCacheFiles() != null) && (context.getCacheFiles().length > 0)) {
                URI[] localPaths = context.getCacheFiles();
                for (URI file : localPaths) {
                    String filename = new Path(file.getPath()).getName();
                    File f = new File(filename);
                    String fileloc = filename;
                    if (!f.exists())
                        fileloc = file.getPath();
                    BufferedReader reader = new BufferedReader(new FileReader(fileloc));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] data = line.split(",");
                        if (!data[0].equals("Store"))
                        {
                            features.put(data[0] + ";" + data[1], Double.valueOf(Double.parseDouble(data[3])));
                        }
                    }
                }
            }
        }

        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException
        {
            String[] data = value.toString().split(",");
            if (data[0].equals("Store")) {
                return;
            }
            if (((Double)features.get(data[0] + ";" + data[2])).doubleValue() > 3.0D) {
                context.write(new Text(data[0] + ";" + data[2]), new Text(data[3]));
            }
        }
    }

    public static class Lab6InMemoryMapSideJoinReducer
            extends Reducer<Text, Text, Text, Text>
    {
        public Lab6InMemoryMapSideJoinReducer() {}

        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException
        {
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            double sum = 0.0D;
            int count = 0;

            for (Text value : values)
            {
                double sales = Double.parseDouble(value.toString());

                if (sales < min) min = sales;
                if (sales > max) { max = sales;
                }
                sum += sales;
                count++;
            }
            double average = sum / count;
            context.write(key, new Text("AVG: " + average+" MIN "+min+" MAX "+max));
        }
    }

    public static void main(String[] args) throws Exception
    {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: lab6.2 <in> <out> <featurefile>");
            System.exit(2);
        }

        String input_path = otherArgs[0];
        String output_path = otherArgs[1];


        new Path(output_path).getFileSystem(conf).delete(new Path(output_path));

        Job job = Job.getInstance(conf, "Lab6InMemoryMapSideJoin");
        job.setJarByClass(Lab6InMemoryMapSideJoin.class);
        job.setMapperClass(Lab6InMemoryMapSideJoinMapper.class);
        job.setReducerClass(Lab6InMemoryMapSideJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.addCacheFile(new URI(otherArgs[2]));
        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

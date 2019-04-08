package ee.ut.cs.ddpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class JobBonus
{
  public JobBonus() {}

  public static void main(String[] args) throws Exception
  {
    org.apache.log4j.BasicConfigurator.configure();
    org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.INFO);

    Configuration conf = new Configuration();
    String[] otherArgs = new org.apache.hadoop.util.GenericOptionsParser(conf, args).getRemainingArgs();

    conf.set("D", otherArgs[2]);

    if (otherArgs.length != 3) {
      System.err.println("Usage: wordcount <in> <out> <D>");
      System.exit(2);
    }

    new Path("outpu1").getFileSystem(conf).delete(new Path("outpu1"));
    new Path("output2").getFileSystem(conf).delete(new Path("output2"));
    new Path(otherArgs[1]).getFileSystem(conf).delete(new Path(otherArgs[1]));


    Job job = new Job(conf, "Word Count");
    job.setJarByClass(Job1WordCount.class);
    job.setMapperClass(Job1WordCount.TokenizerMapper.class);
    job.setCombinerClass(Job1WordCount.IntSumReducer.class);
    job.setReducerClass(Job1WordCount.IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(org.apache.hadoop.io.IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path("output1"));
    job.waitForCompletion(true);


    Job job2 = new Job(conf, "Word Frequency");
    job2.setJarByClass(Job2WordFrequency.class);
    job2.setMapperClass(Job2WordFrequency.TokenizerMapper.class);
    job2.setReducerClass(Job2WordFrequency.IntSumReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    job2.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.class);
    FileInputFormat.addInputPath(job2, new Path("output1"));
    FileOutputFormat.setOutputPath(job2, new Path("output2"));
    job2.waitForCompletion(true);



    Job job3 = new Job(conf, "Bonus Task");
    job3.setJarByClass(Job3TotalFrequency.class);
    job3.setMapperClass(Job3TotalFrequency.TokenizerMapper.class);
    job3.setReducerClass(Job3TotalFrequency.IntSumReducer.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    job3.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.class);
    FileInputFormat.addInputPath(job3, new Path("output2"));
    FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1]));
    System.exit(job3.waitForCompletion(true) ? 0 : 1);
  }
}
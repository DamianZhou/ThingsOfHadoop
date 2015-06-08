package MyDemo.TotalSortDemo;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

public final class TotalSortMRMain {
  public static void main(String... args) throws Exception {
    runSortJob(args);
  }

  public static void runSortJob(String ... args)
      throws Exception {

    int numReducers = 2;
    Path input = new Path(args[0]);
    Path partitionFile = new Path(args[1]);
    Path output = new Path(args[2]);

    InputSampler.Sampler<Text, Text> sampler = 
    		new InputSampler.RandomSampler<Text,Text> (
    				0.1,				//The probability that a key will be picked from the input.
    				10000,			//The number of samples to extract from the input.
    				10);				//The maximum number of input splits that will be read to extract the samples.

    JobConf job = new JobConf();

    job.setNumReduceTasks(numReducers); //Set the number of reducers (which is used by the InputSampler when creating the partition file).

    job.setInputFormat(KeyValueTextInputFormat.class);	//Set the InputFormat for the job, which the InputSampler uses to retrieve records from the input
    job.setOutputFormat(TextOutputFormat.class);
    job.setPartitionerClass(TotalOrderPartitioner.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    TotalOrderPartitioner.setPartitionFile(job, partitionFile); //Specify the location of the partition file.

    
    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    //Run the InputSampler code to sample and create the partition file. 
    //This code uses all the items set in the JobConf object to perform this task.
    InputSampler.writePartitionFile(job, sampler);

    job.setJarByClass(TotalSortMRMain.class);

    output.getFileSystem(job).delete(output, true);

    JobClient.runJob(job);
  }
}

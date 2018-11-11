package pt.lucas2010.hadoop.mapreduce.datacap.analyzer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* Find thread restart logs to produce a timeline */
public class ThreadSegregatorJob extends Configured implements Tool {
  private ThreadSegregatorJob() {}                               // singleton

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("ThreadSegreagatorJob <inDir> <outDir>");
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

//    Path tempDir =
//      new Path("threadRestartAnalyzer-temp-"+
//          Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    Configuration conf = getConf();
//    conf.set(RegexMapper.PATTERN, args[2]);
//    if (args.length == 4)
//      conf.set(RegexMapper.GROUP, args[3]);

    Job threadSegregatorJob = Job.getInstance(conf);
    
    try {
      
      threadSegregatorJob.setJobName("threadSegregator-");
  	  threadSegregatorJob.setJarByClass(ThreadSegregatorJob.class);

      FileInputFormat.setInputPaths(threadSegregatorJob, args[0]);

      threadSegregatorJob.setMapperClass(ThreadSegregationMapper.class);

//      threadRestartAnalyzerJob.setCombinerClass(LongSumReducer.class);
      threadSegregatorJob.setReducerClass(ThreadSegregationReducer.class);

//      FileOutputFormat.setOutputPath(threadSegregatorJob, new Path(args[1]));
//      grepJob.setOutputFormatClass(SequenceFileOutputFormat.class);
      threadSegregatorJob.setOutputKeyClass(Text.class);
      threadSegregatorJob.setOutputValueClass(Text.class);
//
//      grepJob.waitForCompletion(true);
//
//      Job sortJob = Job.getInstance(conf);
//      sortJob.setJobName("grep-sort");
//      sortJob.setJarByClass(Grep.class);

//      FileInputFormat.setInputPaths(sortJob, tempDir);
//      sortJob.setInputFormatClass(SequenceFileInputFormat.class);

//      sortJob.setMapperClass(InverseMapper.class);

//      sortJob.setNumReduceTasks(1);                 // write a single file
      FileOutputFormat.setOutputPath(threadSegregatorJob, new Path(args[1]));
//      sortJob.setSortComparatorClass(          // sort by decreasing freq
//        LongWritable.DecreasingComparator.class);

      
//      // Defines additional single text based output 'text' for the job
//      MultipleOutputs.addNamedOutput(threadSegregatorJob, "text", TextOutputFormat.class,
//      LongWritable.class, Text.class);
//
//      // Defines additional sequence-file based output 'sequence' for the job
//      MultipleOutputs.addNamedOutput(job, "seq",
//        SequenceFileOutputFormat.class,
//        LongWritable.class, Text.class);
      
      
      threadSegregatorJob.waitForCompletion(true);
    }
    finally {
//      FileSystem.get(conf).delete(tempDir, true);
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ThreadSegregatorJob(), args);
    System.exit(res);
  }

}

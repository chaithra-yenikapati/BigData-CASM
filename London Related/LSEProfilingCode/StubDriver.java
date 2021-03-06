import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.io.Text ; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job ; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat ; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class StubDriver {

  public static void main(String[] args) throws Exception {

    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 2) {
      System.err.println("Usage: StubDriver <input dir> <output dir>\n");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    Job job = new Job(conf,"Data Profiling");
    job.setJarByClass(StubDriver.class);job.setJobName("Data Profiling");
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] status_list = fs.listStatus(new Path(args[0]));
    if(status_list  != null){
    	for(FileStatus status: status_list)
    	{
    		FileInputFormat.addInputPath(job, status.getPath());
    		}
    	}
    
    FileOutputFormat.setOutputPath(job,new Path(args[1]));
      job.setMapperClass(StubMapper.class);
      job.setReducerClass(StubReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setNumReduceTasks(0);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
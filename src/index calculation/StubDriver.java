import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StubDriver {

  public static void main(String[] args) throws Exception {

    
	  Configuration conf= new Configuration();
		conf.set("divisor","0");
		Job job = new Job(conf); 
		job.setJarByClass(StubDriver.class);
	    job.setJobName("Index Calculation");
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(StubMapper.class);
		job.setReducerClass(StubReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


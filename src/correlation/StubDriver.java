import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class StubDriver {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
	      System.err.println("Usage: Index Calculation <input path> <output path>");
	      System.exit(-1);
	    }
		String inputPath = args[0];
		String outputPath = args[1]+0;
		
	Job job = new Job();
	Job job1 = new Job();
	job.setJarByClass(StubDriver.class); job.setJobName("pagerank");
	FileInputFormat.addInputPath(job, new Path(inputPath)); 
	FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    job.setMapperClass(StubMapper.class);
	    job.setReducerClass(StubReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    inputPath = outputPath;
	    outputPath = outputPath.substring(0, outputPath.length()-1)+1;
	    
	    while(true){
	    if(job.waitForCompletion(true))
		job1.setJarByClass(StubDriver.class); //job.setJobName("pagerank1");
		FileInputFormat.addInputPath(job1, new Path(inputPath)); 
		FileOutputFormat.setOutputPath(job1, new Path(outputPath));
		    job1.setMapperClass(mapper.class);
		    job1.setReducerClass(reducer.class);
		    job1.setOutputKeyClass(Text.class);
		    job1.setOutputValueClass(Text.class);
		    while(true){
		    	if(job1.waitForCompletion(true))
		    		break;
		    }
		    break;
	    }
	    
	}
	

}
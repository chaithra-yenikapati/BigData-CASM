import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
public class StubMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
    /*
     * TODO implement
     */
	StringTokenizer str = new StringTokenizer(value.toString());
	while(str.hasMoreTokens()){
		context.write(new Text(str.nextToken().trim()), new Text(str.nextToken().trim()));
	}
  }
}

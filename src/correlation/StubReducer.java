import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StubReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    /*
     * TODO implement
     */
	  String output = key.toString().trim();
	  int count = 0;
	  for(Text input:values){
		  output+=","+(input.toString());
		  count++;
	  }
	  if(count == 2){
		  context.write(new Text(output),null);
	  }

  }
}
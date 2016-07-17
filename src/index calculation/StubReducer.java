import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StubReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    /*
     * TODO implement
     */
	  double capSum = 0.0;
	  int count = 0;
	  for(Text input:values){
		double entry = Double.parseDouble(input.toString());
		if(entry>0){
			count++;
		}
		capSum+=entry;
	  }
	  if(count>0){
		  Configuration conf = context.getConfiguration();
		  double index;
		  double divisor = Double.parseDouble(conf.get("divisor"));
		  double normalizedSum = (30/count)*capSum;
		  if(divisor==0){
			  index = 100;
			  divisor = normalizedSum/index;
			  conf.set("divisor", ""+divisor);
			  
		  }
		  else{
			  index = normalizedSum/divisor;
			 // divisor = normalizedSum/index;
			 
		  }
		  
		  context.write(key, new Text(""+index));
		 
	  }

  }
}
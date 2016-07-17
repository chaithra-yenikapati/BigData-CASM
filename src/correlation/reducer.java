import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer extends Reducer<Text, Text, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    /*
     * TODO implement
     */
	  double sigmax=0.0;
	  double sigmay = 0.0;
	  double sigxsq = 0.0;
	  double sigysq = 0.0;
	  double sigmaxy = 0.0;
	  int count = 0;
	  for(Text value: values){
		  String[] input = value.toString().split(",");
		  if(input.length==2){
			  double index1=Double.parseDouble(input[0]);
			  double index2=Double.parseDouble(input[1]);
			  sigmax+= index1;
			  sigmay+= index2;
			  sigxsq+=(index1*index1);
			  sigysq+=(index2*index2);
			  sigmaxy+=index1*index2;
			  count++;
		  }
	  }
	  if(count>0){
		  double ssx = sigxsq - ((sigmax*sigmax)/count);
		  double ssy = sigysq - ((sigmay*sigmay)/count);
		  double ssxy = sigmaxy - ((sigmax*sigmay)/count);
		  double correlation = ssxy/(Math.sqrt(ssx*ssy));
		  context.write(key,new DoubleWritable(correlation));
	  }
	  
  }
}
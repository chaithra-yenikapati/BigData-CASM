import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class StubMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

 
  public double round(String str){
	  return Math.round(Double.parseDouble(str)*100.0)/100.0;
  }
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	
    String[] input = (value.toString().replace("\"","")).split(",");
    if(!input[0].equals("")){
    String[] dateArray = (input[0].trim()).split("/");
    String date = "";
    for(int i=0; i<dateArray.length; i++){
    	date += dateArray[i];
    }
    if(input[2].equals(""))
    	input[2]+="0";
    if(input[3].equals(""))
    	input[3]+="0";
    if(input[4].equals(""))
    	input[4]+="0";
    if(input[5].equals(""))
    	input[5]+="0";
    if(input[1].equals(""))
    	input[1]+=(Double.parseDouble(input[4])+Double.parseDouble(input[5]))/2;
    context.write(new Text(date+","+round(input[3])+","+round(input[1])+","+round(input[5])+","+round(input[4])+","+input[2]), null);
    }
  }
}

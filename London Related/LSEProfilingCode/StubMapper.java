import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;

//import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class StubMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)

	throws IOException, InterruptedException {

		String[] tokens = value.toString().split(",");

		// Date Formatting
		try {
			if (!tokens[1].equalsIgnoreCase("open")) {
				String dates = tokens[0];
				String[] date = dates.split("-");
				int year = Integer.parseInt(date[0].substring(0,4));
				if(year>=2006){
				String output = "";
				for (String str : date)
					output = output + str;
				// System.out.println(output);
				tokens[0] = output;
				// Precision to two decimal places
				double open = Double.parseDouble(tokens[1]);
				String preciseopen = String.format("%.2f", open*1.46);
				tokens[1] = preciseopen;

				double high = Double.parseDouble(tokens[2]);
				String precisehigh = String.format("%.2f", high*1.46);
				tokens[2] = precisehigh;

				double low = Double.parseDouble(tokens[3]);
				String preciselow = String.format("%.2f", low*1.46);
				tokens[3] = preciselow;

				double close = Double.parseDouble(tokens[4]);
				String preciseclose = String.format("%.2f", close*1.46);
				tokens[4] = preciseclose;

				double volume = Double.parseDouble(tokens[5]);
				String precisevolume = String.format("%.2f", volume);
				tokens[5] = precisevolume;

				if (tokens[1].equals(""))
					tokens[1] += "0";
				if (tokens[2].equals(""))
					tokens[2] += "0";
				if (tokens[3].equals(""))
					tokens[3] += "0";
				if (tokens[5].equals(""))
					tokens[5] += "0";
				if (tokens[4].equals(""))
					tokens[4] += (Double.parseDouble(tokens[2]) + Double
							.parseDouble(tokens[3])) / 2;
				context.write(new Text(tokens[0] + "," + tokens[1] + ","
						+ tokens[4] + "," + tokens[3] + "," + tokens[2] + ","
						+ tokens[5]), null);
				}
			}
		} catch (Exception e) {

		}
	}
}

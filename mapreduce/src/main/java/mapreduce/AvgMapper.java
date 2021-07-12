package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] input = value.toString().split(",");
		if (input[0] != null && ! input[0].toString().isEmpty() && input[0] != "student_id") {
			
			context.write(new IntWritable(Integer.parseInt(input[0].toString())), new IntWritable(Integer.parseInt(input[5])));
		}
		
	}
	

}

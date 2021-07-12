package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Reducer;

public class AvgCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable[]>{

	@Override
	protected void reduce(IntWritable arg0, Iterable<IntWritable> arg1,
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable[]>.Context context) throws IOException, InterruptedException {
		Integer count = 0;
		Integer sum = 0;
		for(IntWritable i: arg1) {
			sum =sum + Integer.parseInt(i.toString());
			count = count + 1;
		}
		IntWritable[] values = {new IntWritable(sum), new IntWritable(count)};
		context.write(arg0, values);
	}
	

}

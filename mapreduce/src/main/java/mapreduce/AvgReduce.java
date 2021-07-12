package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Reducer;

public class AvgReduce extends Reducer<IntWritable, IntWritable[], IntWritable, DoubleWritable>{

	@Override
	protected void reduce(IntWritable arg0, Iterable<IntWritable[]> arg1,
			Reducer<IntWritable, IntWritable[], IntWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
		Integer count = 0;
		Integer sum = 0;
		for(IntWritable[] i : arg1) {
			sum = sum + Integer.parseInt(i[0].toString());
			count = count + Integer.parseInt(i[1].toString());
		}
		context.write(arg0, new DoubleWritable(sum/count));
		
	}
	
	

}

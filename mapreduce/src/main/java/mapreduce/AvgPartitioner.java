package mapreduce;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Partitioner;

public class AvgPartitioner extends Partitioner<IntWritable, IntWritable[]> {

	@Override
	public int getPartition(IntWritable arg0, IntWritable[] arg1, int arg2) {
		if(Integer.parseInt(arg0.toString())%2 == 0) {
			return 0;
		}
		return 1;
	}
	

}

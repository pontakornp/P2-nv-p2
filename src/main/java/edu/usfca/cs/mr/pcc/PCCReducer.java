package edu.usfca.cs.mr.pcc;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Value;

import edu.usfca.cs.mr.writables.RunningStatisticsND;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class PCCReducer
extends Reducer<NullWritable, RunningStatisticsND, NullWritable, RunningStatisticsND> {
	private RunningStatisticsND result = new RunningStatisticsND();

    @Override
    protected void reduce(
    		NullWritable key, Iterable<RunningStatisticsND> values, Context context)
    throws IOException, InterruptedException {
    	for (RunningStatisticsND val: values) {
    		result.merge(val);
    	}
    	context.write(key, result);
    }
}

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

    @Override
    protected void reduce(
    		NullWritable key, Iterable<RunningStatisticsND> values, Context context)
    throws IOException, InterruptedException {
    	RunningStatisticsND result = new RunningStatisticsND();
    	for (RunningStatisticsND value: values) {
    		result.merge(value);
    	}
    	context.write(null, result);
    }
}

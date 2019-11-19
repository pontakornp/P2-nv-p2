package edu.usfca.cs.mr.dryingout;

import edu.usfca.cs.mr.writables.DryingOutWritable;
import edu.usfca.cs.mr.writables.ExtremesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class DyringOutReducer
extends Reducer<Text, DryingOutWritable, Text, DryingOutWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<DryingOutWritable> values, Context context)
    throws IOException, InterruptedException {

        float totalWetness = 0;
        int count = 0;
        for (DryingOutWritable val : values) {
            totalWetness += Float.parseFloat(val.getWETNESS().toString());
            count += 1;
        }
        float avgWetness = totalWetness / count;
        DryingOutWritable result = new DryingOutWritable();
        result.setWETNESS(new FloatWritable(avgWetness));
        context.write(key, result);
    }
}

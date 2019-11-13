package edu.usfca.cs.mr.correlation;

import edu.usfca.cs.mr.writables.ExtremesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class CorrelationReducer
extends Reducer<Text, ExtremesWritable, Text, ExtremesWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<ExtremesWritable> values, Context context)
    throws IOException, InterruptedException {

        String tempKey = key.toString();
        ExtremesWritable result = null;
        int keyNum;
        if (tempKey.equals("minAirTemp")) {
            keyNum = 1;
        } else if (tempKey.equals("maxAirTemp")) {
            keyNum = 2;
        } else if (tempKey.equals("minSurfaceTemp")) {
            System.out.println("minSurfaceTemp Reducer");
            keyNum = 3;
        } else { // if (tempKey.equals("maxSurfaceTemp")) {
            keyNum = 4;
        }
        double minTemp = Double.MAX_VALUE;
        double maxTemp = Double.MIN_VALUE;
        double airTemp;
        double surfaceTemp;
        for (ExtremesWritable val : values) {
            airTemp = Double.parseDouble(val.getAIR_TEMPERATURE().toString());
            surfaceTemp = Double.parseDouble(val.getSURFACE_TEMPERATURE().toString());
            if (keyNum == 1 && minTemp > airTemp) {
                result = new ExtremesWritable();
                result.setUTC_DATE(new Text(val.getUTC_DATE()));
                result.setUTC_TIME(new Text(val.getUTC_TIME()));
                result.setLONGITUDE(new DoubleWritable(Double.parseDouble(val.getLONGITUDE().toString())));
                result.setLATITUDE(new DoubleWritable(Double.parseDouble(val.getLATITUDE().toString())));
                result.setAIR_TEMPERATURE(new DoubleWritable(Double.parseDouble(val.getAIR_TEMPERATURE().toString())));
                result.setSURFACE_TEMPERATURE(new DoubleWritable(Double.parseDouble(val.getSURFACE_TEMPERATURE().toString())));
                result.setAIR_TEMPERATURE(new DoubleWritable(airTemp));
                minTemp = airTemp;
            } else if (keyNum == 2 && maxTemp < airTemp) {
                result = new ExtremesWritable();
                result.setUTC_DATE(new Text(val.getUTC_DATE()));
                result.setUTC_TIME(new Text(val.getUTC_TIME()));
                result.setLONGITUDE(new DoubleWritable(Double.parseDouble(val.getLONGITUDE().toString())));
                result.setLATITUDE(new DoubleWritable(Double.parseDouble(val.getLATITUDE().toString())));
                result.setAIR_TEMPERATURE(new DoubleWritable(Double.parseDouble(val.getAIR_TEMPERATURE().toString())));
                result.setSURFACE_TEMPERATURE(new DoubleWritable(Double.parseDouble(val.getSURFACE_TEMPERATURE().toString())));
                maxTemp = airTemp;

            } else if (keyNum == 3 && minTemp > surfaceTemp) {
                result = new ExtremesWritable();
                result.setUTC_DATE(new Text(val.getUTC_DATE()));
                result.setUTC_TIME(new Text(val.getUTC_TIME()));
                result.setLONGITUDE(new DoubleWritable(Double.parseDouble(val.getLONGITUDE().toString())));
                result.setLATITUDE(new DoubleWritable(Double.parseDouble(val.getLATITUDE().toString())));
                result.setAIR_TEMPERATURE(new DoubleWritable(Double.parseDouble(val.getAIR_TEMPERATURE().toString())));
                result.setSURFACE_TEMPERATURE(new DoubleWritable(Double.parseDouble(val.getSURFACE_TEMPERATURE().toString())));
                minTemp = surfaceTemp;
            } else if (keyNum == 4 && maxTemp < surfaceTemp) {
                result = new ExtremesWritable();
                result.setUTC_DATE(new Text(val.getUTC_DATE()));
                result.setUTC_TIME(new Text(val.getUTC_TIME()));
                result.setLONGITUDE(new DoubleWritable(Double.parseDouble(val.getLONGITUDE().toString())));
                result.setLATITUDE(new DoubleWritable(Double.parseDouble(val.getLATITUDE().toString())));
                result.setAIR_TEMPERATURE(new DoubleWritable(Double.parseDouble(val.getAIR_TEMPERATURE().toString())));
                result.setSURFACE_TEMPERATURE(new DoubleWritable(Double.parseDouble(val.getSURFACE_TEMPERATURE().toString())));
                result.setSURFACE_TEMPERATURE(new DoubleWritable(surfaceTemp));
                maxTemp = surfaceTemp;
            }
        }

        if (result != null) {
            System.out.println("-----------------");
            System.out.print(key.toString());
            System.out.println(result.getAIR_TEMPERATURE());
            System.out.println(result.getSURFACE_TEMPERATURE());
            System.out.println("-----------------");
            context.write(key, result);
        }
    }
}

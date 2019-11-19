package edu.usfca.cs.mr.extremes;

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
public class ExtremesReducer
extends Reducer<Text, ExtremesWritable, Text, ExtremesWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<ExtremesWritable> values, Context context)
    throws IOException, InterruptedException {

        String tempKey = key.toString();
        int keyNum;
        if (tempKey.equals("minAirTemp")) {
            keyNum = 1;
        } else if (tempKey.equals("maxAirTemp")) {
            keyNum = 2;
        } else if (tempKey.equals("minSurfaceTemp")) {
            keyNum = 3;
        } else { // if (tempKey.equals("maxSurfaceTemp")) {
            keyNum = 4;
        }
        double minTemp = Double.MAX_VALUE;
        double maxTemp = Double.MIN_VALUE;
        double curAirTemp = 0;
        double curSurfaceTemp = 0;

        String utcDate = "";
        String utcTime = "";
        double longitude = 0;
        double latitude = 0;
        double airTemp = 0;
        double surfaceTemp = 0;
        for (ExtremesWritable val : values) {
            curAirTemp = Double.parseDouble(val.getAIR_TEMPERATURE().toString());
            curSurfaceTemp = Double.parseDouble(val.getSURFACE_TEMPERATURE().toString());
            if (keyNum == 1 && minTemp > curAirTemp) {
                utcDate = val.getUTC_DATE().toString();
                utcTime = val.getUTC_TIME().toString();
                longitude = Double.parseDouble(val.getLONGITUDE().toString());
                latitude = Double.parseDouble(val.getLATITUDE().toString());
                airTemp = curAirTemp;
                surfaceTemp = curSurfaceTemp;
                minTemp = airTemp;
            } else if (keyNum == 2 && maxTemp < curAirTemp) {
                utcDate = val.getUTC_DATE().toString();
                utcTime = val.getUTC_TIME().toString();
                longitude = Double.parseDouble(val.getLONGITUDE().toString());
                latitude = Double.parseDouble(val.getLATITUDE().toString());
                airTemp = curAirTemp;
                surfaceTemp = curSurfaceTemp;
                maxTemp = airTemp;
            } else if (keyNum == 3 && minTemp > curSurfaceTemp) {
                utcDate = val.getUTC_DATE().toString();
                utcTime = val.getUTC_TIME().toString();
                longitude = Double.parseDouble(val.getLONGITUDE().toString());
                latitude = Double.parseDouble(val.getLATITUDE().toString());
                airTemp = curAirTemp;
                surfaceTemp = curSurfaceTemp;
                minTemp = surfaceTemp;
            } else if (keyNum == 4 && maxTemp < curSurfaceTemp) {
                utcDate = val.getUTC_DATE().toString();
                utcTime = val.getUTC_TIME().toString();
                longitude = Double.parseDouble(val.getLONGITUDE().toString());
                latitude = Double.parseDouble(val.getLATITUDE().toString());
                airTemp = curAirTemp;
                surfaceTemp = curSurfaceTemp;
                maxTemp = surfaceTemp;
            }
        }

        if (!utcDate.isEmpty()) {
            ExtremesWritable result = new ExtremesWritable();
            result.setUTC_DATE(new Text(utcDate));
            result.setUTC_TIME(new Text(utcTime));
            result.setLONGITUDE(new DoubleWritable(longitude));
            result.setLATITUDE(new DoubleWritable(latitude));
            result.setAIR_TEMPERATURE(new DoubleWritable(airTemp));
            result.setSURFACE_TEMPERATURE(new DoubleWritable(surfaceTemp));
            context.write(key, result);
        }
    }
}

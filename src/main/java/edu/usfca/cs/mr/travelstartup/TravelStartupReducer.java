package edu.usfca.cs.mr.travelstartup;

import edu.usfca.cs.mr.util.SanFranciscoWeather;
import edu.usfca.cs.mr.writables.MovingOutWritable;
import edu.usfca.cs.mr.writables.TravelStartupWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class TravelStartupReducer
extends Reducer<IntWritable, TravelStartupWritable, IntWritable, TravelStartupWritable> {

    @Override
    protected void reduce(
            IntWritable key, Iterable<TravelStartupWritable> values, Context context)
    throws IOException, InterruptedException {
        String geoHash;
        double airTemp;
        double humidity;
        Map<String, Double> geoHashAirTemp = new HashMap<>();
        Map<String, Double> geoHashHumidity = new HashMap<>();
        Map<String, Integer> geoHashCount = new HashMap<>();
        for(TravelStartupWritable val: values) {
            geoHash = val.getGeoHash().toString();
            airTemp = Double.parseDouble(val.getAIR_TEMPERATURE().toString());
            humidity = Double.parseDouble(val.getRELATIVE_HUMIDITY().toString());
            if (geoHashAirTemp.containsKey(geoHash) && geoHashHumidity.containsKey(geoHash)) {
                geoHashAirTemp.put(geoHash, geoHashAirTemp.get(geoHash) + airTemp);
                geoHashHumidity.put(geoHash, geoHashHumidity.get(geoHash) + humidity);
                geoHashCount.put(geoHash, geoHashCount.get(geoHash) + 1);
            } else {
                geoHashAirTemp.put(geoHash,  airTemp);
                geoHashHumidity.put(geoHash, humidity);
                geoHashCount.put(geoHash, 1);
            }
        }
        TravelStartupWritable result;
        double avgAirTemp;
        double avgHumidity;
        double fahrenheitAvgAirTemp;
        double comfortIndex;
        for (String geoHashKey: geoHashAirTemp.keySet()) {
            avgAirTemp = geoHashAirTemp.get(geoHashKey) / geoHashCount.get(geoHashKey);
            avgHumidity = geoHashHumidity.get(geoHashKey) / geoHashCount.get(geoHashKey);
            // Comfort Index = (temperature + relative humidity)/4
            // temperature in Fahrenheit
            fahrenheitAvgAirTemp = (avgAirTemp * 9/5) + 32;
            comfortIndex = (fahrenheitAvgAirTemp + avgHumidity) / 4;
            int monthNum = Integer.parseInt(key.toString());
            result = new TravelStartupWritable();
            result.set(new IntWritable(monthNum), new Text(geoHashKey), new DoubleWritable(avgAirTemp), new DoubleWritable(avgHumidity), new DoubleWritable(comfortIndex));
            context.write(key, result);
        }
    }
}

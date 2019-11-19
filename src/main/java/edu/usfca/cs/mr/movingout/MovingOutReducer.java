package edu.usfca.cs.mr.movingout;

import edu.usfca.cs.mr.util.SanFranciscoWeather;
import edu.usfca.cs.mr.writables.ExtremesWritable;
import edu.usfca.cs.mr.writables.MovingOutWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class MovingOutReducer
extends Reducer<Text, MovingOutWritable, Text, IntWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<MovingOutWritable> values, Context context)
    throws IOException, InterruptedException {
        
        double[] airTempArr = new double[12];
        double[] precipitationArr = new double[12];
        double[] humidityArr = new double[12];

        int[] monthCount = new int[12];
        for (MovingOutWritable val : values) {
            String localDate = val.getLST_DATE().toString();
            double airTemp = Double.parseDouble(val.getAIR_TEMPERATURE().toString());
            double precipitation = Double.parseDouble(val.getPRECIPITATION().toString());
            double humidity = Double.parseDouble(val.getRELATIVE_HUMIDITY().toString());
            int monthNum = Integer.parseInt(localDate.substring(4, 6));
            int monthNumIndex = monthNum - 1;
            airTempArr[monthNumIndex] += airTemp;
            precipitationArr[monthNumIndex] += precipitation;
            humidityArr[monthNumIndex] += humidity;
            monthCount[monthNumIndex]++;
        }
        int count = 0;
        for (int i = 0; i < 12; i++) {
            double sfAirTemp = SanFranciscoWeather.SF_AIR_TEMPERATURE[i];
            double sfPrecipitation = SanFranciscoWeather.SF_PRECIPITATION[i];
            double sfHumidity = SanFranciscoWeather.SF_RELATIVE_HUMIDITY[i];

            if (monthCount[i] > 0) {
                double airTemp = airTempArr[i] / monthCount[i];
                double precipitation = precipitationArr[i] / monthCount[i];
                double humidity = humidityArr[i] / monthCount[i];
                if (((sfAirTemp - 5 <= airTemp) && (airTemp <= sfAirTemp + 5)) &&
                        (precipitation <= sfPrecipitation) &&
                        ((sfHumidity - 15 <= humidity) && (humidity <= sfHumidity + 15))) {
                    count++;
                }
            }
        }
        if (count >= 5) {
            context.write(key, new IntWritable(count));
        }
    }
}

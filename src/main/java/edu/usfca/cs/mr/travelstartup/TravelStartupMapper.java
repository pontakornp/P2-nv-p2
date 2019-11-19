package edu.usfca.cs.mr.travelstartup;

import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.NcdcConstants;
import edu.usfca.cs.mr.writables.MovingOutWritable;
import edu.usfca.cs.mr.writables.TravelStartupWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 *
 *
 * Field#  Name                           Units / Type
 * ---------------------------------------------------------------
 *    1    WBANNO                         XXXXX / STRING
 *    2    UTC_DATE                       YYYYMMDD / STRING
 *    3    UTC_TIME                       HHmm / STRING
 *    4    LST_DATE                       YYYYMMDD / STRING
 *    5    LST_TIME                       HHmm / STRING
 *    6    CRX_VN                         XXXXXX / INT
 *    7    LONGITUDE                      Decimal_/ degrees DOUBLE
 *    8    LATITUDE                       Decimal_degrees / DOUBLE
 *    9    AIR_TEMPERATURE                Celsius / DOUBLE
 *    10   PRECIPITATION                  mm / DOUBLE
 *    11   SOLAR_RADIATION                W/m^2 / DOUBLE
 *    12   SR_FLAG                        X / INT
 *    13   SURFACE_TEMPERATURE            Celsius / DOUBLE
 *    14   ST_TYPE                        X / STRING
 *    15   ST_FLAG                        X / INT
 *    16   RELATIVE_HUMIDITY              % / STRING
 *    17   RH_FLAG                        X / INT
 *    18   SOIL_MOISTURE_5                m^3/m^3 / DOUBLE
 *    19   SOIL_TEMPERATURE_5             Celsius / DOUBLE
 *    20   WETNESS                        Ohms / DOUBLE
 *    21   WET_FLAG                       X / INT
 *    22   WIND_1_5                       m/s / DOUBLE
 *    23   WIND_FLAG                      X / INT
 */
public class TravelStartupMapper
extends Mapper<LongWritable, Text, IntWritable, TravelStartupWritable> {

    private TravelStartupWritable travelStartupWritable = new TravelStartupWritable();

    private Text LST_DATE = new Text();
    private FloatWritable LONGITUDE = new FloatWritable();
    private FloatWritable LATITUDE = new FloatWritable();
    private DoubleWritable AIR_TEMPERATURE = new DoubleWritable();
    private DoubleWritable RELATIVE_HUMIDITY = new DoubleWritable();
    private IntWritable RH_FLAG = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // split line into string array
        String[] fields = value.toString().split("\\s+");

        String LST_DATE_STRING = fields[NcdcConstants.LST_DATE_INDEX];
        float LONGITUDE_FLOAT = Float.parseFloat(fields[NcdcConstants.LONGITUDE_INDEX]);
        float LATITUDE_FLOAT = Float.parseFloat(fields[NcdcConstants.LATITUDE_INDEX]);
        double AIR_TEMPERATURE_DOUBLE = Double.parseDouble(fields[NcdcConstants.AIR_TEMPERATURE_INDEX]);
        double RELATIVE_HUMIDITY_DOUBLE = Double.parseDouble(fields[NcdcConstants.RELATIVE_HUMIDITY_INDEX]);
        int RH_FLAG_INT = Integer.parseInt(fields[NcdcConstants.RH_FLAG_INDEX]);

        LST_DATE.set(LST_DATE_STRING);
        LONGITUDE.set(LONGITUDE_FLOAT);
        LATITUDE.set(LATITUDE_FLOAT);
        AIR_TEMPERATURE.set(AIR_TEMPERATURE_DOUBLE);
        RELATIVE_HUMIDITY.set(RELATIVE_HUMIDITY_DOUBLE);
        RH_FLAG.set(RH_FLAG_INT);

        if (RH_FLAG_INT == 0 && !(LONGITUDE_FLOAT == NcdcConstants.MISSING_DATA_1 ||
                LATITUDE_FLOAT == NcdcConstants.MISSING_DATA_1 ||
                AIR_TEMPERATURE_DOUBLE == NcdcConstants.MISSING_DATA_1 ||
                RELATIVE_HUMIDITY_DOUBLE == NcdcConstants.MISSING_DATA_1 ||
                LONGITUDE_FLOAT == NcdcConstants.MISSING_DATA_2 ||
                LATITUDE_FLOAT == NcdcConstants.MISSING_DATA_2 ||
                AIR_TEMPERATURE_DOUBLE == NcdcConstants.MISSING_DATA_2 ||
                RELATIVE_HUMIDITY_DOUBLE == NcdcConstants.MISSING_DATA_2)) {
            String geoHash = Geohash.encode(LONGITUDE_FLOAT, LATITUDE_FLOAT, 4);
            Set<String> pickedGeoHash = new HashSet<>();
            pickedGeoHash.add("h2jb");
            pickedGeoHash.add("h9d3");
            pickedGeoHash.add("hb02");
            pickedGeoHash.add("hbh2");
            pickedGeoHash.add("hbk3");
            if (pickedGeoHash.contains(geoHash)) {
                String monthNumString = LST_DATE_STRING.substring(4, 6);
                int monthNum = Integer.parseInt(monthNumString);
                travelStartupWritable.set(new IntWritable(monthNum), new Text(geoHash), AIR_TEMPERATURE, RELATIVE_HUMIDITY, new DoubleWritable(0));
                context.write(new IntWritable(monthNum), travelStartupWritable);
            }
        }
    }
}

package edu.usfca.cs.mr.dryingout;

import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.writables.DryingOutWritable;
import edu.usfca.cs.mr.writables.ExtremesWritable;
import edu.usfca.cs.mr.util.NcdcConstants;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

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
public class DryingOutMapper
extends Mapper<LongWritable, Text, Text, DryingOutWritable> {
    private DryingOutWritable dryingOutWritable = new DryingOutWritable();

    private Text UTC_DATE = new Text();
    private FloatWritable LONGITUDE = new FloatWritable();
    private FloatWritable LATITUDE = new FloatWritable();
    private FloatWritable WETNESS = new FloatWritable();
    private IntWritable WET_FLAG = new IntWritable();

    private String regionGeomap = "";
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // split line into string array
        String[] fields = value.toString().split("\\s+");

        String UTC_DATE_STRING = fields[NcdcConstants.UTC_DATE_INDEX];
        float LONGITUDE_FLOAT = Float.parseFloat(fields[NcdcConstants.LONGITUDE_INDEX]);
        float LATITUDE_FLOAT = Float.parseFloat(fields[NcdcConstants.LATITUDE_INDEX]);
        float WETNESS_FLOAT = Float.parseFloat(fields[NcdcConstants.WETNESS_INDEX]);
        int WET_FLAG_INT = Integer.parseInt(fields[NcdcConstants.WET_FLAG_INDEX]);

        UTC_DATE.set(UTC_DATE_STRING);
        LONGITUDE.set(LONGITUDE_FLOAT);
        LATITUDE.set(LATITUDE_FLOAT);
        WETNESS.set(WETNESS_FLOAT);
        WET_FLAG.set(WET_FLAG_INT);

        dryingOutWritable.set(UTC_DATE, LONGITUDE, LATITUDE, WETNESS, WET_FLAG);
        if (!(LONGITUDE_FLOAT == NcdcConstants.MISSING_DATA_1 ||
                LATITUDE_FLOAT == NcdcConstants.MISSING_DATA_1 ||
                LONGITUDE_FLOAT == NcdcConstants.MISSING_DATA_2 ||
                LATITUDE_FLOAT == NcdcConstants.MISSING_DATA_2) && WET_FLAG_INT == 0) {
            String geoHash = Geohash.encode(LATITUDE_FLOAT, LONGITUDE_FLOAT, 4);
            // Santa Babara
            if (geoHash.equals("9q4g")) {
                String monthNum = UTC_DATE_STRING.substring(4, 6);
                context.write(new Text(monthNum), dryingOutWritable);
            }
        }
    }
}

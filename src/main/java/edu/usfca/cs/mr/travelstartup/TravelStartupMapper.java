package edu.usfca.cs.mr.travelstartup;

import edu.usfca.cs.mr.writables.ExtremesWritable;
import edu.usfca.cs.mr.util.NcdcConstants;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

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
extends Mapper<LongWritable, Text, Text, ExtremesWritable> {
    private ExtremesWritable extremesWritable = new ExtremesWritable();

    private Text UTC_DATE = new Text();
    private Text UTC_TIME = new Text();
    private DoubleWritable LONGITUDE = new DoubleWritable();
    private DoubleWritable LATITUDE = new DoubleWritable();
    private DoubleWritable AIR_TEMPERATURE = new DoubleWritable();
    private DoubleWritable SURFACE_TEMPERATURE = new DoubleWritable();

    private double minAirTemp = Double.MAX_VALUE;
    private double maxAirTemp = Double.MIN_VALUE;
    private double minSurfaceTemp = Double.MAX_VALUE;
    private double maxSurfaceTemp = Double.MIN_VALUE;

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // split line into string array
        String[] fields = value.toString().split("\\s+");

        String UTC_DATE_STRING = fields[NcdcConstants.UTC_DATE_INDEX];
        String UTC_TIME_STRING = fields[NcdcConstants.UTC_TIME_INDEX];
        double LONGITUDE_DOUBLE = Double.parseDouble(fields[NcdcConstants.LONGITUDE_INDEX]);
        double LATITUDE_DOUBLE = Double.parseDouble(fields[NcdcConstants.LATITUDE_INDEX]);
        double AIR_TEMPERATURE_DOUBLE = Double.parseDouble(fields[NcdcConstants.AIR_TEMPERATURE_INDEX]);
        double SURFACE_TEMPERATURE_DOUBLE = Double.parseDouble(fields[NcdcConstants.SURFACE_TEMPERATURE_INDEX]);

        UTC_DATE.set(UTC_DATE_STRING);
        UTC_TIME.set(UTC_TIME_STRING);
        LONGITUDE.set(LONGITUDE_DOUBLE);
        LATITUDE.set(LATITUDE_DOUBLE);
        AIR_TEMPERATURE.set(AIR_TEMPERATURE_DOUBLE);
        SURFACE_TEMPERATURE.set(SURFACE_TEMPERATURE_DOUBLE);

        extremesWritable.set(UTC_DATE, UTC_TIME, LONGITUDE, LATITUDE, AIR_TEMPERATURE, SURFACE_TEMPERATURE);
        if (!(LONGITUDE_DOUBLE == NcdcConstants.MISSING_DATA_1 ||
                LATITUDE_DOUBLE == NcdcConstants.MISSING_DATA_1 ||
                AIR_TEMPERATURE_DOUBLE == NcdcConstants.MISSING_DATA_1 ||
                SURFACE_TEMPERATURE_DOUBLE == NcdcConstants.MISSING_DATA_1 ||
                LONGITUDE_DOUBLE == NcdcConstants.MISSING_DATA_2 ||
                LATITUDE_DOUBLE == NcdcConstants.MISSING_DATA_2 ||
                AIR_TEMPERATURE_DOUBLE == NcdcConstants.MISSING_DATA_2 ||
                SURFACE_TEMPERATURE_DOUBLE == NcdcConstants.MISSING_DATA_2)) {
            if (minAirTemp > AIR_TEMPERATURE_DOUBLE) {
//                System.out.println("1 minAirTemp Mapper");

                context.write(new Text("minAirTemp"), extremesWritable);
                minAirTemp = AIR_TEMPERATURE_DOUBLE;
            }

            if (maxAirTemp < AIR_TEMPERATURE_DOUBLE) {
//                System.out.println("2 maxAirTemp Mapper");

                context.write(new Text("maxAirTemp"), extremesWritable);
                maxAirTemp = AIR_TEMPERATURE_DOUBLE;
            }

            if (minSurfaceTemp > SURFACE_TEMPERATURE_DOUBLE) {

//                System.out.println("3 minSurfaceTemp Mapper");

                context.write(new Text("minSurfaceTemp"), extremesWritable);
                minSurfaceTemp = SURFACE_TEMPERATURE_DOUBLE;
            }

            if (maxSurfaceTemp < SURFACE_TEMPERATURE_DOUBLE) {
                if (SURFACE_TEMPERATURE_DOUBLE > 50) {
                    System.out.println(SURFACE_TEMPERATURE_DOUBLE);
                }
//                System.out.println("4 maxSurfaceTemp Mapper");

                context.write(new Text("maxSurfaceTemp"), extremesWritable);
                maxSurfaceTemp = SURFACE_TEMPERATURE_DOUBLE;

            }

        }
    }
}

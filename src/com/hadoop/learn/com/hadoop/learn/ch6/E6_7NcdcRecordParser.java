package com.hadoop.learn.com.hadoop.learn.ch6;

import org.apache.hadoop.io.Text;

/**
 * @author Ly on 2018/7/6.
 */
public class E6_7NcdcRecordParser {

    private static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private int airTemperature;
    private String quality;
    private boolean isMalformed;

    public void parse(String record) {
        year = record.substring(15, 19);
        String airTemperatureString = String.valueOf(Integer.MIN_VALUE);

        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
        } else if (record.charAt(87) == '-') {
            airTemperatureString = record.substring(87, 92);
        } else {
            isMalformed = true;
        }

        if (!isMalformed) {
            airTemperature = Integer.parseInt(airTemperatureString);
            quality = record.substring(92, 93);
        }
    }

    public void parse(Text text) {
        parse(text.toString());
    }

    public boolean isValidTemperature() {
        return airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
    }

    public String getYear() {
        return year;
    }

    public int getAirTemperature() {
        return airTemperature;
    }

    public boolean isMalformed() {
        return isMalformed;
    }
}

package org.apache.flink.connector.jdbc.table.utils.hash;

import org.apache.flink.connector.jdbc.table.utils.hash.util.PGTm;
import org.apache.flink.connector.jdbc.table.utils.hash.util.UInt32;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.connector.jdbc.table.utils.hash.GreenplumCdbHash.hashAny;
import static org.apache.flink.connector.jdbc.table.utils.hash.GreenplumCdbHash.hashUInt32;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

public class GreenplumHashFunc {
    public static final int DEC_DIGITS = 4;

    public static final int POSTGRES_EPOCH_JDATE = 2451545;

    public static UInt32 hashChar(char k) {
        return hashUInt32(new UInt32(k));
    }

    public static UInt32 hashBoolean(char k) {
        if (k == '0') {
            return hashChar((char) 0);
        } else {
            return hashChar((char) 1);
        }
    }

    /** used for smallint */
    public static UInt32 hashInt2(int k) {
        return hashUInt32(new UInt32(k));
    }

    /** used for integer */
    public static UInt32 hashInt4(int k) {
        return hashUInt32(new UInt32(k));
    }

    /** used for bigint */
    public static UInt32 hashInt8(long val) {
        UInt32 lohalf = new UInt32((int) val);
        UInt32 hihalf = new UInt32((int) (val >> 32));

        if (val >= 0) {
            lohalf = lohalf.xor(hihalf);
        } else {
            lohalf = lohalf.xor(hihalf.not());
        }

        return hashUInt32(lohalf);
    }

    public static UInt32 hashBpChar(String val, Charset charset) {
        byte[] inputBytes = val.getBytes(charset);

        return hashAny(inputBytes, inputBytes.length);
    }

    public static UInt32 hashText(String val, Charset charset) {
        byte[] inputBytes = val.getBytes(charset);

        return hashAny(inputBytes, inputBytes.length);
    }

    /**
     * timeHash rewrite function time_hash in adbpg.
     */
    public static UInt32 timeHash(String val, boolean useIntegerDatetime) {
        // the unit in time is microsecond
        long julianTimeOffset;

        // in date time
        DateTimeFormatterBuilder dateTimeFormatterBuilder  = new DateTimeFormatterBuilder()
                .appendPattern("HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
                .optionalEnd();

        LocalTime time = LocalTime.parse(val, dateTimeFormatterBuilder.toFormatter());
        int fractionalSecond = 0;
        if (useIntegerDatetime) {
            fractionalSecond = time.getNano() / 1000;
        } else {
            fractionalSecond = time.getNano() / 1000000;
        }

        julianTimeOffset = tm2time(time.getHour(), time.getMinute(), time.getSecond(), fractionalSecond, useIntegerDatetime);
        if (useIntegerDatetime) {
            return hashInt8(julianTimeOffset);
        } else {
            return hashFloat8((double) julianTimeOffset);
        }
    }

    /**
     * timetzHash function rewrites timetz_hash in adbpg and just makes compatible
     * with format like '0000:00:00+08:00 GMT'
     * Attention: the same time in time_hash and timetz_hash is not same.
     */
    public static UInt32 timetzHash(String val, boolean useIntegerDatetime) {
        long julianTimeOffset;
        long timeZone;
        int fractionalSecond;
        // in date time
        DateTimeFormatterBuilder dateTimeFormatterBuilder  = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .appendValue(HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(MINUTE_OF_HOUR, 2)
                .appendLiteral(':')
                .appendValue(SECOND_OF_MINUTE, 2)
                .optionalStart()
                .appendFraction(NANO_OF_SECOND, 0, 9, true)
                .optionalEnd()
                .optionalStart()
                .appendLiteral(" ")
                .optionalEnd()
                .optionalStart()
                .appendOffsetId()
                .optionalEnd();

        if (val.contains("+") || val.contains("-")) {
            OffsetTime offsetDateTime = OffsetTime.parse(val, dateTimeFormatterBuilder.toFormatter());
            if (useIntegerDatetime) {
                fractionalSecond = offsetDateTime.getNano() / 1000;
            } else {
                fractionalSecond = offsetDateTime.getNano() / 1000000;
            }
            julianTimeOffset = tm2time(offsetDateTime.getHour(), offsetDateTime.getMinute(), offsetDateTime.getSecond(), fractionalSecond, useIntegerDatetime);
            timeZone = -offsetDateTime.getOffset().getTotalSeconds();
        } else {
            LocalTime time = LocalTime.parse(val, dateTimeFormatterBuilder.toFormatter());
            if (useIntegerDatetime) {
                fractionalSecond = time.getNano() / 1000;
            } else {
                fractionalSecond = time.getNano() / 1000000;
            }
            julianTimeOffset = tm2time(time.getHour(), time.getMinute(), time.getSecond(), fractionalSecond, useIntegerDatetime);
            timeZone = 0;
        }

        UInt32 thash = new UInt32(0);
        if (useIntegerDatetime) {
            thash = hashInt8(julianTimeOffset);
        } else {
            thash = hashFloat8(julianTimeOffset);
        }

        return thash.xor(hashUInt32(new UInt32((int) timeZone)));
    }

    /**
     * dateHash will first convert calendar time to Julian date and calculate it
     * using hashInt4.
     * Note: we just support date in format like 'yyyy-MM-dd'.
     * In postgresql date / timestamp is julian date, so we use 4713 BC as the beginning of time.
     */
    public static UInt32 dateHash(String val) {
        // trim input string and get the first 10 characters
        String dateString = val.trim().substring(0, 10);

        DateTimeFormatterBuilder dateTimeFormatterBuilder = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .appendPattern("yyyy-MM-dd");

        DateTimeFormatter dateTimeFormatter = dateTimeFormatterBuilder.toFormatter();
        LocalDate dateTime = LocalDate.parse(dateString, dateTimeFormatter);

        long julianDate = date2j(dateTime.getYear(), dateTime.getMonthValue(), dateTime.getDayOfMonth()) - POSTGRES_EPOCH_JDATE;
        // convert julianDate to int
        return hashInt4((int) julianDate);
    }

    public static long tm2time(int hour, int min, int second, int fractionalSecond, boolean useIntegerDatetime) {
        long result;
        if (useIntegerDatetime) {
            result = (((hour * 60L + min) * 60) + second) * 1000000L + fractionalSecond;
        } else {
            result = (hour * 60L + min) * 60 + second + fractionalSecond;
        }
        return result;
    }

    public static UInt32 timestampHash(String val, boolean useIntegerDatetime) {
        long julianTimeOffset;

        // parse LocalTime to PGTm
        PGTm pgTm = parseTimestampWithoutTimeZoneUsingPattern(val);
        int fractionalSecond = 0;
        if (useIntegerDatetime) {
            fractionalSecond = pgTm.getFractionalSecond() / 1000;
        } else {
            fractionalSecond = pgTm.getFractionalSecond() / 1000000;
        }
        julianTimeOffset = tm2timestamp(pgTm, fractionalSecond, 0, useIntegerDatetime);
        if (useIntegerDatetime) {
            return hashInt8(julianTimeOffset);
        } else {
            return hashFloat8((double) julianTimeOffset);
        }
    }

    public static UInt32 timestamptzHash(String val, boolean useIntegerDatetime, String gpCurrentUTCOffset) {
        long julianTimeOffset;
        PGTm pgTm;

        // Because we just support input timestamp like '2023-12-12 00:00:00+08:00'
        // or '2023-12-12 00:00:00 +08:00'. So we use a regex here to make sure input string
        // has time zone information.
        // I think it is not elegant.
        Pattern pattern = Pattern.compile(".*[+-][0-9][0-9]:[0-9][0-9]");
        Matcher matcher = pattern.matcher(val);
        if (matcher.find()) {
            pgTm = parseTimestampWithTimeZoneUsingPattern(val, gpCurrentUTCOffset);
        } else {
            pgTm = parseTimestampWithoutTimeZoneUsingPattern(val);
        }

        int fractionalSecond = 0;
        if (useIntegerDatetime) {
            fractionalSecond = pgTm.getFractionalSecond() / 1000;
        } else {
            fractionalSecond = pgTm.getFractionalSecond() / 1000000;
        }

        julianTimeOffset = tm2timestamp(pgTm, fractionalSecond, (int) pgTm.getTmZone(), useIntegerDatetime);
        if (useIntegerDatetime) {
            return hashInt8(julianTimeOffset);
        } else {
            return hashFloat8((double) julianTimeOffset);
        }
    }


    public static PGTm parseTimestampWithoutTimeZoneUsingPattern(String val) {
        PGTm pgTm = new PGTm();

        DateTimeFormatterBuilder dateTimeFormatterBuilder = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6,  true)
                .optionalEnd();

        DateTimeFormatter dateTimeFormatter = dateTimeFormatterBuilder.toFormatter();
        LocalDateTime dateTime = LocalDateTime.parse(val, dateTimeFormatter);

        pgTm.setTmYear(dateTime.getYear());
        pgTm.setTmMon(dateTime.getMonthValue());
        pgTm.setTmMDay(dateTime.getDayOfMonth());

        pgTm.setTmHour(dateTime.getHour());
        pgTm.setTmMin(dateTime.getMinute());
        pgTm.setTmSec(dateTime.getSecond());
        pgTm.setFractionalSecond(dateTime.getNano());

        return pgTm;
    }

    /**
     * parseTimestampWithoutTimeZoneUsingPattern is used to parse timestamp string to PGTm.
     * Note: To compatible with dts, just support data format like '2023-12-12 01:12:12.123456 +08:00 GMT'.
     * UTC and GMT should be considered equivalent, so UTC and GMT is optional
     * TODO(@yiwei): compatible more date format
     *
     * @param val
     *        input timestamp string
     * @param gpCurrentUTCOffset
     *        time offset with UTC, we get it from pg_timezone_names.
     */
    public static PGTm parseTimestampWithTimeZoneUsingPattern(String val, String gpCurrentUTCOffset) {
        PGTm pgTm = new PGTm();

        DateTimeFormatterBuilder dateTimeFormatterBuilder  = new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
                .optionalEnd()
                .optionalStart()
                .appendLiteral(" ")
                .optionalEnd()
                .optionalStart()
                .appendOffsetId()
                .optionalEnd();

        DateTimeFormatter dateTimeFormatter = dateTimeFormatterBuilder.toFormatter();
        OffsetDateTime dateTime = OffsetDateTime.parse(val, dateTimeFormatter);

        pgTm.setTmYear(dateTime.getYear());
        pgTm.setTmMon(dateTime.getMonthValue());
        pgTm.setTmMDay(dateTime.getDayOfMonth());

        pgTm.setTmHour(dateTime.getHour());
        pgTm.setTmMin(dateTime.getMinute());
        pgTm.setTmSec(dateTime.getSecond());
        pgTm.setFractionalSecond(dateTime.getNano());

        // parse gpCurrentUTCOffset and re-calculate time zone offset
        // equation: pgTm.tmZone = (time zone in input string) - (gp current UTC Offset)
        gpCurrentUTCOffset = gpCurrentUTCOffset.trim();
        int signBit = 1;
        int charOffset = 0;
        int hour = 0;
        int minute = 0;
        StringBuilder buffer = new StringBuilder();
        while (charOffset < gpCurrentUTCOffset.length()) {
            if (charOffset == 0) {
                if (gpCurrentUTCOffset.charAt(charOffset) == '+') {
                    signBit = 1;
                } else if (gpCurrentUTCOffset.charAt(charOffset) == '-') {
                    signBit = -1;
                } else {
                    // TODO(@yiwei): throws exception to upper level
                    signBit = 0; // make ide quiet
                }
                charOffset++;
                continue;
            }

            if (gpCurrentUTCOffset.charAt(charOffset) == ':') {
                hour = Integer.parseInt(buffer.toString());
                buffer = new StringBuilder();
            } else {
                buffer.append(gpCurrentUTCOffset.charAt(charOffset));
            }
            charOffset++;
        }
        minute = Integer.parseInt(buffer.toString());
        int pgTmZone = dateTime.getOffset().getTotalSeconds() - signBit * ((hour * 60 * 60) + minute * 60);
        pgTm.setTmZone(pgTmZone);
        return pgTm;
    }

    public static long tm2timestamp(PGTm pgTm, int fractionalSecond, int timeZone, boolean useIntegerDatetime) {
        long result;

        long date = date2j(pgTm.getTmYear(), pgTm.getTmMon(), pgTm.getTmMDay()) - POSTGRES_EPOCH_JDATE; // date2j(2000, 1, 1)
        long time = time2t(pgTm.getTmHour(), pgTm.getTmMin(), pgTm.getTmSec(), fractionalSecond, useIntegerDatetime);
        if (useIntegerDatetime) {
            result = date * 86400000000L + time;
        } else {
            result = date * 86400 + time;
        }

        if (timeZone != 0 && useIntegerDatetime) {
            result = result - (timeZone * 1000000L);
        } else if (timeZone != 0) {
            result = result - timeZone;
        }
        return result;
    }

    /**
     * Calendar time to Julian date conversions.
     * Julian day counts from 0 to 2147483647 (Nov 24, -4713 to Jun 3, 5874898)
     */
    public static long date2j(int year, int month, int day) {
        int julian;
        int century;

        if (month > 2) {
            month = month + 1;
            year = year + 4800;
        } else {
            month = month + 13;
            year = year + 4799;
        }

        century = year / 100;
        julian = year * 365 - 32167;
        julian = julian + (year / 4 - century + century / 4);
        julian = julian + (7834 * month / 256 + day);
        return julian;
    }

    public static long time2t(int hour, int min, int second, int fractionalSecond, boolean useIntegerDatetime) {
        long timeOffset;
        if (useIntegerDatetime) {
            timeOffset = (((((hour * 60L) + min) * 60L) + second) * 1000000L) + fractionalSecond;
        } else {
            timeOffset = (((hour * 60L) + min) * 60L) + second + fractionalSecond;
        }
        return timeOffset;
    }

    public static UInt32 hashFloat4(String val) {
        return hashFloat4(Float.parseFloat(val));
    }

    public static UInt32 hashFloat4(float val) {
        if (val == 0) {
            return new UInt32(0);
        }

        long k = Double.doubleToLongBits(val);
        char[] input = longToCharArray(k);

        return hashAny(input, input.length);
    }

    public static UInt32 hashFloat8(String val) {
        return hashFloat8(Double.parseDouble(val));
    }

    public static UInt32 hashFloat8(double val) {
        if (val == 0) {
            return new UInt32(0);
        }
        long k = Double.doubleToLongBits(val);
        char[] input = longToCharArray(k);

        return hashAny(input, input.length);
    }

    /**
     * hashNumeric rewrites function hash_numeric in adbpg.
     * Attention:
     * 1) We do not take 1E-10 into consideration.
     * 2) we don't hash on Numeric's scale, since two numerics can
     * compare equal but have different scales. We also don't hash on the sign,
     * although we could: since a sign difference implies inequality, this
     * shouldn't affect correctness. (So we do not calculate dscale and sign here).
     */
    public static UInt32 hashNumeric(String val) {
        // trim input value
        val = val.trim();
        int dWeight = 0;
        /*
         * unsigned char tdd[NUMERIC_LOCAL_DTXT], NUMERIC_LOCAL_DTXT=128
         * if val.length + DEC_DIGITS * 2 > NUMERIC_LOCAL_DTXT
         * palloc a new decDigits
         */
        List<Character> decDigits = new ArrayList<>();

        /*
         * init numeric var from input string
         * trim zero input value.
         */
        int zeroPosition = 0;
        for (int i = 0; i < val.length(); i++) {
            if (val.charAt(i) != '0') {
                zeroPosition = i;
                break;
            }
        }

        boolean haveDp = false;
        for (int i = zeroPosition; i < val.length(); i++) {
            if (val.charAt(i) >= '0' && val.charAt(i) <= '9') {
                decDigits.add((char) (val.charAt(i) - '0'));
                if (!haveDp) {
                    dWeight++;
                }
            } else if (val.charAt(i) == '.') {
                haveDp = true;
            } else {
                break;
            }
        }

        // get digits > 0
        int digitBeforeDpNumber = (dWeight + DEC_DIGITS - 1) / DEC_DIGITS;

        short[] digitsBeforeDp;
        // if number before dope is zero, add zero to digitBeforeDp
        if (digitBeforeDpNumber == 0) {
            digitsBeforeDp = new short[]{(short) 0};
            digitBeforeDpNumber = 1;
        } else {
            digitsBeforeDp = new short[digitBeforeDpNumber];
        }

        int j = 1;
        if (dWeight % 4 != 0) {
            j = 5 - dWeight % 4;
        }

        short digitValue = 0;
        int offset = 0;
        boolean finishedOneDigit = false;
        for (int i = 0; i < dWeight; i++) {
            digitValue = (short) (digitValue * 10 + decDigits.get(i));
            if (j == 4) {
                digitsBeforeDp[offset++] = digitValue;
                digitValue = 0;
                j = 1;
                finishedOneDigit = true;
            } else {
                finishedOneDigit = false;
                j++;
            }
        }

        if (!finishedOneDigit) {
            digitsBeforeDp[offset] = digitValue;
        }
        // create an int array and init while we have dope in input string
        int digitAfterDpNumber  = (decDigits.size() - dWeight + DEC_DIGITS - 1) / DEC_DIGITS;
        short[] digitsAfterDp = new short[digitAfterDpNumber];

        // init offset here
        offset = 0;
        j = 1;
        digitValue = 0;
        finishedOneDigit = false;
        if (haveDp) {
            for (int i = dWeight; i < decDigits.size(); i++) {
                digitValue = (short) (digitValue * 10 + decDigits.get(i));
                if (j == 4) {
                    digitsAfterDp[offset++] = digitValue;
                    digitValue = 0;
                    j = 1;
                    finishedOneDigit = true;
                } else {
                    j++;
                    finishedOneDigit = false;
                }
            }

            if (!finishedOneDigit) {
                short bowValue = (short) Math.pow(10, DEC_DIGITS - j + 1);
                digitsAfterDp[offset] = (short) (digitValue * bowValue);
            }
        }

        /*
         * trim zero value in digitsBeforeDp and digitsAfterDp and
         * mark it using startOffset(digitBeforeDp) and endOffset(digitAfterDp)
         */
        int startOffset = 0;
        int endOffset = 0;
        j = 0;
        while (j < digitBeforeDpNumber) {
            if (digitsBeforeDp[j] != 0) {
                break;
            }
            j++;
        }

        startOffset = j;

        j = digitAfterDpNumber - 1;
        while (j >= 0) {
            if (digitsAfterDp[j] != 0) {
                break;
            }
            j--;
        }
        endOffset = j;

        int trimZeroBeforeDpSize = 0;
        if (endOffset < 0) {
            for (int i = digitsBeforeDp.length - 1; i >= startOffset; i--) {
                if (digitsBeforeDp[i] != 0) {
                    break;
                }
                trimZeroBeforeDpSize++;
            }
        }

        int weight = 0;
        if (dWeight >= 0) {
            weight = (dWeight + DEC_DIGITS - 1) / DEC_DIGITS - 1;
        } else {
            weight = -((-dWeight - 2) / DEC_DIGITS + 1);
        }

        // if weight < 0, truncate leading zeros in digitAfterDp, and re-calculate weight
        int trimZeroAfterDpSize = 0;
        if (weight < 0) {
            for (int i = 0; i <= endOffset; i++) {
                if (digitsAfterDp[i] != 0) {
                    break;
                }
                trimZeroAfterDpSize++;
                weight--;
            }
        }

        if (endOffset < 0 && startOffset == digitBeforeDpNumber) {
            return new UInt32(-1);
        }

        StringBuilder digitArray = new StringBuilder();
        for (int i = startOffset; i < digitsBeforeDp.length - trimZeroBeforeDpSize; i++) {
            digitArray.append(shortToCharArray(digitsBeforeDp[i]));
        }

        for (int i = trimZeroAfterDpSize; i <= endOffset; i++) {
            digitArray.append(shortToCharArray(digitsAfterDp[i]));
        }

        int hashLen = (digitsBeforeDp.length - trimZeroBeforeDpSize - startOffset) + (endOffset + 1 - trimZeroAfterDpSize);

        UInt32 digitHash = hashAny(digitArray.toString().toCharArray(), hashLen * DEC_DIGITS / 2);

        return digitHash.xor(new UInt32(weight));
    }

    public static char[] shortToCharArray(short val) {
        // init a char array which size is 2
        char[] res = new char[2];
        /*
         * type Short do not have toBinaryString function, so use Integer instead.
         * In toBinaryString function, it will not pad with zeros according to
         * size of the type in bytes.
         */
        StringBuilder c = new StringBuilder(Integer.toBinaryString(val));
        int zeroLength = 0;
        if (c.length() < 16) {
            zeroLength = 16 - c.length();
        }

        for (int i = 0; i < zeroLength; i++) {
            c.insert(0, "0");
        }
        int arrayOffset = 0;
        int resOffset = 1;
        int[] inputArray = new int[8];

        for (int i = 0; i < c.length(); i++) {
            inputArray[arrayOffset] = c.charAt(i) == '1' ? 1 : 0;
            if (arrayOffset == 7) {
                res[resOffset] = intArrayToChar(inputArray);
                inputArray = new int[8];
                arrayOffset = 0;
                resOffset--;
            } else {
                arrayOffset++;
            }
        }

        return res;
    }

    public static char[] longToCharArray(long val) {
        // init a char array which size is 8
        char[] res = new char[8];
        StringBuilder c = new StringBuilder(Long.toBinaryString(val));
        int zeroLength = 0;
        if (c.length() < 64) {
            zeroLength = 64 - c.length();
        }

        // give zero at the head of string
        for (int i = 0; i < zeroLength; i++) {
            c.insert(0, "0");
        }

        int arrayOffset = 0;
        int resOffset = 7;
        int[] inputArray = new int[8];
        for (int i = 0; i < c.length(); i++) {
            inputArray[arrayOffset] = c.charAt(i) == '1' ? 1 : 0;
            if (arrayOffset == 7) {
                res[resOffset] = intArrayToChar(inputArray);
                inputArray = new int[8];
                arrayOffset = 0;
                resOffset--;
            } else {
                arrayOffset++;
            }
        }
        return res;
    }

    // given a binary input int array, return char
    public static char intArrayToChar(int[] inputArray) {
        int calResult = 0;
        for (int i = 0; i < 8; i++) {
            calResult = calResult + inputArray[i] * (int) Math.pow(2, 8 - i - 1);
        }
        return (char) calResult;
    }
}

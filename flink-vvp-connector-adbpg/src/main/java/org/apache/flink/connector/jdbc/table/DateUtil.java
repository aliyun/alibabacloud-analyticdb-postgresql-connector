package org.apache.flink.connector.jdbc.table;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple2;

import javax.annotation.Nonnull;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DateUtil
 */
public class DateUtil {
    private static ConcurrentHashMap<String, FastDateFormat> sdfCache = new ConcurrentHashMap<String, FastDateFormat>() {
        {
            this.put("yyyy-MM-dd HH:mm:ss", FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss"));
            this.put("yyyy-MM-dd HH:mm:ss.SSS", FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS"));
            this.put("yyyy-MM-dd", FastDateFormat.getInstance("yyyy-MM-dd"));
            this.put("HH:mm:ss", FastDateFormat.getInstance("HH:mm:ss"));
        }
    };

    public DateUtil() {
    }

    private static FastDateFormat getDateFormat(String timeZone, String format) {
        String key;
        if (null != timeZone && !timeZone.isEmpty()) {
            key = timeZone + format;
        } else {
            key = String.valueOf(format);
        }

        if (sdfCache.containsKey(key)) {
            return (FastDateFormat) sdfCache.get(key);
        } else {
            FastDateFormat sdf = FastDateFormat.getInstance(format, TimeZone.getTimeZone(timeZone));
            sdfCache.put(key, sdf);
            return sdf;
        }
    }

    public static String timeStamp2String(Timestamp value, String timeZone, boolean reserveMs) {
        String format;
        if (reserveMs) {
            format = "yyyy-MM-dd HH:mm:ss.SSS";
        } else {
            format = "yyyy-MM-dd HH:mm:ss";
        }

        return timeStamp2String(value, timeZone, format);
    }

    public static String timeStamp2String(Timestamp value, String timeZone, @Nonnull String format) {
        FastDateFormat sdf = getDateFormat(timeZone, format);
        return sdf.format(value);
    }

    public static String date2String(Date value, String timeZone) {
        return date2String(value, timeZone, "yyyy-MM-dd");
    }

    public static String date2String(Date value, String timeZone, @Nonnull String format) {
        FastDateFormat sdf = getDateFormat(timeZone, format);
        return sdf.format(value);
    }

    public static String time2String(Time time, String timeZone) {
        return time2String(time, timeZone, "HH:mm:ss");
    }

    public static String time2String(Time time, String timeZone, @Nonnull String format) {
        FastDateFormat sdf = getDateFormat(timeZone, format);
        return sdf.format(time);
    }

    public static Long parseDateString(String formatString, String dateString, String timeZone) throws ParseException {
        FastDateFormat simpleDateFormat = getDateFormat(timeZone, formatString);
        return simpleDateFormat.parse(dateString).getTime();
    }

    public static boolean isTimeInRange(List<Tuple2<Long, Long>> rangeList, long time) {
        Iterator var3 = rangeList.iterator();

        Tuple2 range;
        do {
            if (!var3.hasNext()) {
                return false;
            }

            range = (Tuple2) var3.next();
        } while ((Long) range.f0 > time || time > (Long) range.f1);

        return true;
    }
}

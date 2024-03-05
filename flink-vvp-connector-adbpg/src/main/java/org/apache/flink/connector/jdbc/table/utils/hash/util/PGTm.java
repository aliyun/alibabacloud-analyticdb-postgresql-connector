package org.apache.flink.connector.jdbc.table.utils.hash.util;

public class PGTm {
    private int tmSec;
    private int tmMin;

    private int tmHour;

    private int tmMDay;

    private int tmMon;

    private int tmYear;

    private int tmWDay;

    private int tmYDay;

    private int tmIsDst;

    private int tmGmtOff;

    private long tmZone;

    // in greenplum, pgTm do not have fractionalSecond because we can pass its value within functions by address.
    // Here the unit of fractional second is nanosecond.
    private int fractionalSecond;
    public int getFractionalSecond() {
        return fractionalSecond;
    }

    public void setFractionalSecond(int fractionalSecond) {
        this.fractionalSecond = fractionalSecond;
    }


    public int getTmSec() {
        return tmSec;
    }

    public void setTmSec(int tmSec) {
        this.tmSec = tmSec;
    }

    public int getTmMin() {
        return tmMin;
    }

    public void setTmMin(int tmMin) {
        this.tmMin = tmMin;
    }

    public int getTmHour() {
        return tmHour;
    }

    public void setTmHour(int tmHour) {
        this.tmHour = tmHour;
    }

    public int getTmMDay() {
        return tmMDay;
    }

    public void setTmMDay(int tmMDay) {
        this.tmMDay = tmMDay;
    }

    public int getTmMon() {
        return tmMon;
    }

    public void setTmMon(int tmMon) {
        this.tmMon = tmMon;
    }

    public int getTmYear() {
        return tmYear;
    }

    public void setTmYear(int tmYear) {
        this.tmYear = tmYear;
    }

    public int getTmWDay() {
        return tmWDay;
    }

    public void setTmWDay(int tmWDay) {
        this.tmWDay = tmWDay;
    }

    public int getTmYDay() {
        return tmYDay;
    }

    public void setTmYDay(int tmYDay) {
        this.tmYDay = tmYDay;
    }

    public int getTmIsDst() {
        return tmIsDst;
    }

    public void setTmIsDst(int tmIsDst) {
        this.tmIsDst = tmIsDst;
    }

    public int getTmGmtOff() {
        return tmGmtOff;
    }

    public void setTmGmtOff(int tmGmtOff) {
        this.tmGmtOff = tmGmtOff;
    }

    public long getTmZone() {
        return tmZone;
    }

    public void setTmZone(long tmZone) {
        this.tmZone = tmZone;
    }
}

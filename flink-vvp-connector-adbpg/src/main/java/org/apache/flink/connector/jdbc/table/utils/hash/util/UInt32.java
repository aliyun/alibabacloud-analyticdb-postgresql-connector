package org.apache.flink.connector.jdbc.table.utils.hash.util;

public class UInt32 extends UInt<UInt32> {
    static final long LONG =  0xffffffffL;

    public UInt32(int value) {
        super(value);
    }

    public UInt32(byte value) {
        super(value & 0xFF);
    }

    @Override
    public UInt32 divide(UInt32 other) {
        long div = (this.uintVal & LONG) / (other.uintVal & LONG);
        int result = (int) div;
        return new UInt32(result);
    }

    @Override
    public UInt32 mod(UInt32 other) {
        return null;
    }

    @Override
    public UInt32 multiply(UInt32 other) {
        long multi = (this.uintVal & LONG) * (other.uintVal & LONG);
        int result = (int) multi;
        return new UInt32(result);
    }

    @Override
    public UInt32 pow(int exp) {
        return null;
    }

    @Override
    public UInt32 not() {
        int result = ~this.uintVal;
        return new UInt32(result);
    }

    @Override
    public UInt32 and(UInt32 other) {
        int result = this.uintVal & other.uintVal;
        return new UInt32(result);
    }

    @Override
    public UInt32 or(UInt32 other) {
        int result = this.uintVal | other.uintVal;
        return new UInt32(result);
    }

    @Override
    public UInt32 xor(UInt32 other) {
        int result = this.uintVal ^ other.uintVal;
        return new UInt32(result);
    }

    @Override
    public UInt32 add(UInt32 other) {
        long sum = (this.uintVal & LONG) + (other.uintVal & LONG);
        int out = (int) sum;
        return new UInt32(out);
    }

    public UInt32 add(char other) {
        long sum = (this.uintVal & LONG) + (other & LONG);
        int out = (int) sum;
        return new UInt32(out);
    }

    public UInt32 add(byte other) {
        long sum = (this.uintVal & LONG) + (other & 0xFF);
        int out = (int) sum;
        return new UInt32(out);
    }

    @Override
    public UInt32 subtract(UInt32 other) {
        long sub = (this.uintVal & LONG) - (other.uintVal & LONG);
        int result = (int) sub;
        return new UInt32(result);
    }

    @Override
    public UInt32 shiftLeft(int places) {
        long lShift = 0;
        if (0 < places) {
            lShift =  (this.uintVal & LONG) << places;
        } else {
            lShift =  (this.uintVal & LONG) >>> -places;
        }
        int result = (int) lShift;
        return new UInt32(result);
    }

    @Override
    public UInt32 shiftRight(int places) {
        long rShift = 0;
        if (0 < places) {
            rShift = (this.uintVal & LONG) >>> places;
        } else {
            rShift = (this.uintVal & LONG) << -places;
        }
        int result = (int) rShift;
        return new UInt32(result);
    }

    @Override
    public int compareTo(UInt32 o) {
        return 0;
    }

    @Override
    public int intValue() {
        return 0;
    }

    @Override
    public long longValue() {
        return this.uintVal & LONG;
    }

    @Override
    public float floatValue() {
        return 0;
    }

    @Override
    public double doubleValue() {
        return 0;
    }
}


package org.apache.flink.connector.jdbc.table.utils.hash.util;


public abstract class UInt<T extends UInt> extends java.lang.Number implements Comparable<T> {
    public int uintVal;

    static final int DEFAULT_RADIX = 10;

    UInt(int value) {
        this.uintVal = value;
    }

    // codes for (this / other)
    public abstract T divide(T other);

    // codes for (this % other)
    public abstract T mod(T other);

    // codes for (this * other)
    public abstract T multiply(T other);

    // codes for (this ** exp)
    public abstract T pow(int exp);

    // codes for (~this)
    public abstract T not();

    // codes for (this & other)
    public abstract T and(T other);

    // codes for (this | other)
    public abstract T or(T other);

    // codes for (this ^ other)
    public abstract T xor(T other);

    // codes for (this + other)
    public abstract T add(T other);


    // codes for (this - other)
    public abstract T subtract(T other);

    // codes for (this << places)
    public abstract T shiftLeft(int places);

    // codes for (this >> places)
    public abstract T shiftRight(int places);
}

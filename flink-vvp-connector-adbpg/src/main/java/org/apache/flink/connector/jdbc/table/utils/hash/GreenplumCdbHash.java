package org.apache.flink.connector.jdbc.table.utils.hash;

import org.apache.flink.connector.jdbc.table.utils.hash.util.JdbcTypes;
import org.apache.flink.connector.jdbc.table.utils.hash.util.UInt32;

import static org.apache.flink.connector.jdbc.table.utils.hash.GreenplumHashFunc.hashBpChar;
import static org.apache.flink.connector.jdbc.table.utils.hash.GreenplumHashFunc.hashFloat4;
import static org.apache.flink.connector.jdbc.table.utils.hash.GreenplumHashFunc.hashFloat8;
import static org.apache.flink.connector.jdbc.table.utils.hash.GreenplumHashFunc.hashInt4;
import static org.apache.flink.connector.jdbc.table.utils.hash.GreenplumHashFunc.hashInt8;
import static org.apache.flink.connector.jdbc.table.utils.hash.GreenplumHashFunc.hashNumeric;
import static org.apache.flink.connector.jdbc.table.utils.hash.GreenplumHashFunc.timeHash;
import static org.apache.flink.connector.jdbc.table.utils.hash.GreenplumHashFunc.timestampHash;
import static org.apache.flink.connector.jdbc.table.utils.hash.GreenplumHashFunc.timestamptzHash;

public class GreenplumCdbHash {
    /**
     * The entry to get target segment id
     * @param values
     *        input values in the form of strings
     * @param isNulls
     *        Mark whether the input value is in a null value. If so,
     *        it is marked as 1, otherwise, it is marked as 0.
     * @param dataType
     *        Mark the data type which input value is. We use types in
     *        com.alibaba.amp.any.jdbc.JdbcTypes here.
     * @param nKeys
     *        The size of input values array. We do not use the size
     *        of values, isNulls, dataType here.
     * @param numSegments
     *        The segment number of target database.
     */
    public static int getTargetSegmentId(String[] values, int[] isNulls, int[] dataType, int nKeys, int numSegments)
    {
        int hash = 0;
        int target_seg = 0;
        if (nKeys <= 0)
            return target_seg;
        for (int i=0; i < nKeys; i++)
        {
            // exchange data type from UInt32 into int type
            hash = cdbHash(new UInt32(hash), values[i], isNulls[i], dataType[i]).uintVal;
        }
        target_seg = (int)cdbHashReduce(new UInt32(hash), numSegments);
        return target_seg;
    }

    /**
     * The following jump consistent hash algorithm is just the one
     * from the original paper:
     * "A Fast, Minimal Memory, Consistent Hash Algorithm"
     */
    public static long jumpConsistentHash(long key, int numSegments) {
        long b = -1;
        long j = 0;

        while (j < numSegments) {
            b = j;
            key = key * 2862933555777941757L + 1;
            // key in jump_consistent_hash is an uint64 type,
            j = (long)((b + 1) * ((double)(1L << 31) / (double)((key >>> 33) + 1))) ;
        }
        return b;
    }

    /**
     * Get a hash value from hash func according to its data type and
     * return a new hash value.
     * @param hash
     *        default hash value or hash value of the previous primary
     *        key. default value is 0.
     * @param value
     *        field value in string format
     * @param isNull
     *        Mark whether the input value is in a null value. If so,
     *        it is marked as 1, otherwise, it is marked as 0.
     * @param type
     *        Mark the data type which input value is. We use types in
     *        com.alibaba.amp.any.jdbc.JdbcTypes here.
     */
    public static UInt32 cdbHash(UInt32 hash, String value, int isNull, int type) {
        UInt32 hashKey = new UInt32(hash.uintVal);

        // hashKey = (hashKey << 1) | ((hashKey & 0x80000000) ? 1 : 0);
        UInt32 orLeft = hashKey.shiftLeft(1);
        UInt32 right = hashKey.and(new UInt32(0x80000000));
        if (right.longValue() > 0) {
            right = new UInt32(1);
        }
        hashKey = orLeft.or(right);

        if (isNull == 0) {
            UInt32 hKey = new UInt32(0);
            switch (type) {
                case JdbcTypes.INTEGER: // distclass = 10048
                    hKey = hashInt4(Integer.parseInt(value));
                    break;
                case JdbcTypes.BIGINT:
                    hKey = hashInt8(Long.parseLong(value));
                    break;
                case JdbcTypes.REAL:
                case JdbcTypes.FLOAT: // real distclass=10054
                    hKey = hashFloat4(Float.parseFloat(value));
                    break;
                case JdbcTypes.DOUBLE:
                    hKey = hashFloat8(Double.parseDouble(value));
                    break;
                case JdbcTypes.DECIMAL:
                case JdbcTypes.NUMERIC:
                    hKey = hashNumeric(value);
                    break;
                case JdbcTypes.VARCHAR:
                    hKey = hashBpChar(value);
                    break;
                case JdbcTypes.TIMESTAMP:
                    hKey = timestampHash(value, true);
                    break;
                case JdbcTypes.TIME:
                    hKey = timeHash(value, true);
                    break;
                case JdbcTypes.TIMESTAMPTIMEZONE:
                    hKey = timestamptzHash(value, true, "+00:00");
                    break;
                case JdbcTypes.DATETIME:
                    break;
                default:
                    throw new RuntimeException(String.format("unexpect jdbc type in cdb hash, jdbc type code %d", type));
            }
            hashKey = hashKey.xor(hKey);
        }
        return hashKey;
    }

    /**
     * A simplified version of function cdbhashreduce in cdbhash.c. In
     * cdbhashreduce function, it provides three hash mode (
     * REDUCE_BITMASK, REDUCE_LAZYMOD and REDUCE_JUMP_HASH). We just
     * implement REDUCE_JUMP_HASH here.
     */
    public static long cdbHashReduce(UInt32 hash, int numSegments)
    {
        long result = 0;
        result = jumpConsistentHash(hash.longValue(), numSegments);
        return result;
    }


    public static UInt32 hashUInt32(UInt32 k) {
        return hashBytesUint32(k);
    }

    /**
     * Hash a variable-length key into a 32-bit value.
     * This is the key function used to get hash values used by bpchar
     * and other variable-length data type.
     * Note: here input chars is not stored in WORDS_BIGENDIAN format.
     * may be in the future, we should take WORDS_BIGENDIAN into
     * consideration.
     *
     * @param k
     *        the key (the unaligned variable-length array of bytes)
     * @param keyLen
     *        the length of the key, counting by size of char array
     */
    public static UInt32 hashAny(char[] k, int keyLen) {
        // set up init value for a, b, c
        UInt32 a = new UInt32(0x9e3779b9).add(new UInt32(keyLen)).add(new UInt32(3923095));
        UInt32 b = new UInt32(0x9e3779b9).add(new UInt32(keyLen)).add(new UInt32(3923095));
        UInt32 c = new UInt32(0x9e3779b9).add(new UInt32(keyLen)).add(new UInt32(3923095));

        int i = 0;
        while (keyLen >= 12) {
            a = a.add(k[i]).add(new UInt32(k[i + 1]).shiftLeft(8)).add(new UInt32(k[i + 2]).shiftLeft(16)).add(new UInt32(k[i + 3]).shiftLeft(24));
            b = b.add(k[i + 4]).add(new UInt32(k[i + 5]).shiftLeft(8)).add(new UInt32(k[i + 6]).shiftLeft(16)).add(new UInt32(k[i + 7]).shiftLeft(24));
            c = c.add(k[i + 8]).add(new UInt32(k[i + 9]).shiftLeft(8)).add(new UInt32(k[i + 10]).shiftLeft(16)).add(new UInt32(k[i + 11]).shiftLeft(24));
            UInt32 [] mixArray = {a, b, c};
           mix(mixArray);
           a = mixArray[0];
           b = mixArray[1];
           c = mixArray[2];
           keyLen = keyLen - 12;
           i = i + 12;
        }

        int j = keyLen + i - 1;
        // it seems that !WORDS_BIGENDIAN
        switch(keyLen) {
            case 11:
                c = c.add(new UInt32(k[j]).shiftLeft(24));
                j--;
            case 10:
                c = c.add(new UInt32(k[j]).shiftLeft(16));
                j--;
            case 9:
                c = c.add(new UInt32(k[j]).shiftLeft(8));
                j--;
            case 8:
                b = b.add(new UInt32(k[j]).shiftLeft(24));
                j--;
            case 7:
                b = b.add(new UInt32(k[j]).shiftLeft(16));
                j--;
            case 6:
                b = b.add(new UInt32(k[j]).shiftLeft(8));
                j--;
            case 5:
                b = b.add(new UInt32(k[j]));
                j--;
            case 4:
                a = a.add(new UInt32(k[j]).shiftLeft(24));
                j--;
            case 3:
                a = a.add(new UInt32(k[j]).shiftLeft(16));
                j--;
            case 2:
                a = a.add(new UInt32(k[j]).shiftLeft(8));
                j--;
            case 1:
                a = a.add(new UInt32(k[j]));
        }
        UInt32 [] finalArray = {a, b, c};
        finalFunc(finalArray);
        return finalArray[2];
    }

    public static UInt32 hashBytesUint32(UInt32 k) {
        UInt32 a = new UInt32(0x9e3779b9).add(new UInt32(4)).add(new UInt32(3923095));
        UInt32 b = new UInt32(0x9e3779b9).add(new UInt32(4)).add(new UInt32(3923095));
        UInt32 c = new UInt32(0x9e3779b9).add(new UInt32(4)).add(new UInt32(3923095));
        a = a.add(k);

        UInt32 [] inputArray = {a, b, c};
        finalFunc(inputArray);

        c = inputArray[2];
        return c;
    }

    /** wrapper for micro rot in cdbhash.c
     * origin definition is : #define rot(x,k) (((x)<<(k)) | ((x)>>(32-(k))))
     * it can rotate x, for example: x = 10001000, k = 1, after rotate x= 00010001
     */
    public static UInt32 rot(UInt32 x, int y) {
        UInt32 left = x.shiftLeft(y);
        UInt32 right = x.shiftRight(32 - y);

        return left.or(right);
    }

    /** wrapper for micro mix in cdbhash.c */
    public static void mix(UInt32 [] inputArray) {
        UInt32 a = inputArray[0];
        UInt32 b = inputArray[1];
        UInt32 c = inputArray[2];
        // a -= c;  a ^= rot(c, 4);	c += b;
        a = a.subtract(c);
        UInt32 rotC4 = GreenplumCdbHash.rot(c, 4);
        a = a.xor(rotC4);
        c = b.add(c);

        // b -= a;  b ^= rot(a, 6);	a += c;
        b = b.subtract(a);
        UInt32 rotA6 = GreenplumCdbHash.rot(a, 6);
        b = b.xor(rotA6);
        a = a.add(c);

        // c -= b;  c ^= rot(b, 8);	b += a;
        c = c.subtract(b);
        UInt32 rotB8 = GreenplumCdbHash.rot(b, 8);
        c = c.xor(rotB8);
        b = b.add(a);

        // a -= c;  a ^= rot(c,16);	c += b;
        a = a.subtract(c);
        UInt32 rotC16 = GreenplumCdbHash.rot(c, 16);
        a = a.xor(rotC16);
        c = c.add(b);

        // b -= a;  b ^= rot(a,19);	a += c;
        b = b.subtract(a);
        UInt32 rotA19 = GreenplumCdbHash.rot(a, 19);
        b = b.xor(rotA19);
        a = a.add(c);

        // c -= b;  c ^= rot(b, 4);	b += a;
        c = c.subtract(b);
        UInt32 rotB4 = GreenplumCdbHash.rot(b, 4);
        c = c.xor(rotB4);
        b = b.add(a);

        // change values in inputArray
        inputArray[0] = a;
        inputArray[1] = b;
        inputArray[2] = c;
    }

    /** wrapper for micro final in cdbhash.c */
    public static void finalFunc(UInt32 [] inputArray) {
        UInt32 a = inputArray[0];
        UInt32 b = inputArray[1];
        UInt32 c = inputArray[2];
        // c ^= b; c -= rot(b,14);
        c = c.xor(b);
        UInt32 rotB14 = GreenplumCdbHash.rot(b, 14);
        c = c.subtract(rotB14);

        // a ^= c; a -= rot(c,11);
        a = a.xor(c);
        UInt32 rotC11 = GreenplumCdbHash.rot(c, 11);
        a = a.subtract(rotC11);

        // b ^= a; b -= rot(a,25);
        b = b.xor(a);
        UInt32 rotA25 = GreenplumCdbHash.rot(a, 25);
        b = b.subtract(rotA25);

        // c ^= b; c -= rot(b,16);
        c = c.xor(b);
        UInt32 rotB16 = GreenplumCdbHash.rot(b, 16);
        c = c.subtract(rotB16);

        // a ^= c; a -= rot(c, 4);
        a = a.xor(c);
        UInt32 rotC4 = GreenplumCdbHash.rot(c, 4);
        a = a.subtract(rotC4);

        // b ^= a; b -= rot(a,14);
        b = b.xor(a);
        UInt32 rotA14 = GreenplumCdbHash.rot(a, 14);
        b = b.subtract(rotA14);

        // c ^= b; c -= rot(b,24);
        c = c.xor(b);
        UInt32 rotB24 = GreenplumCdbHash.rot(b, 24);
        c = c.subtract(rotB24);

        // change values in inputArray
        inputArray[0] = a;
        inputArray[1] = b;
        inputArray[2] = c;
    }


}

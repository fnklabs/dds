package com.fnklabs.dds;

import java.nio.ByteBuffer;

public class BytesUtils {
    /**
     * Compare left and right bytes. length of left and right bytes array must be equals
     * <p>
     * Return 1  if left > right
     * Return -1 if  left < right
     * Return 0  if left = right
     *
     * @param left
     * @param right
     *
     * @return
     */
    public static int compare(byte[] left, byte[] right) {
        for (int i = 0; i < left.length; i++) {
            byte leftByte = left[i];
            byte rightByte = right[i];

            int compareResult = Byte.compare(leftByte, rightByte);

            if (compareResult != 0) {
                return compareResult;
            }
        }

        return 0;
    }

    public static void read(ByteBuffer src, ByteBuffer dst) {
        while (dst.remaining() > 0 && src.remaining() > 0) {
            dst.put(src.get());
        }
    }

}

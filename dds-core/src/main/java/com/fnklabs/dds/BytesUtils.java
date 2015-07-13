package com.fnklabs.dds;

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
            int leftByte = left[i] & 0xFF;
            int rightByte = right[i] & 0xFF;

            if (leftByte > rightByte) {
                return 1;
            } else if (leftByte < rightByte) {
                return -1;
            }
        }
        return 0;
    }
}

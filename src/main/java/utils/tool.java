package utils;

public class tool {

    public static long bytesToLong(byte[] bytes) {
        long num = 0;
        int len = bytes.length;
        for (int ix = 0; ix < 8; ++ix) {
            num <<= 8;
            num |= (bytes[len - ix - 1] & 0xff);
        }
        return num;
    }

    public static byte[] longToBytes(long num) {
        byte[] byteNum = new byte[8];
        for (int ix = 0; ix < 8; ++ix) {
            int offset = 64 - (ix + 1) * 8;
            byteNum[ix] = (byte) ((num >> offset) & 0xff);
        }
        return byteNum;
    }

    public static boolean Xor_Empty(byte[] xor) {
        for (int i = 0; i < xor.length; i++) {
            if (xor[i] == 0) {
                continue;
            } else
                return false;
        }
        return true;
    }


    public static byte[] Xor(byte[] x, byte[] y) {
        int min =0;
        if(x.length>y.length){
            min = y.length;
        }else{
            min = x.length;
        }
        byte[] temp = new byte[min];
        for (int i = 0; i < min; i++) {
            temp[i] = (byte) (x[i] ^ y[i]);
        }
        return temp;
    }
}
package utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class Hash {

    private static Random random = new Random();

    public static void setSeed(long seed) {
        random.setSeed(seed);
    }

    public static long hash64(long x, long seed) {
        x += seed;
        x = (x ^ (x >>> 33)) * 0xff51afd7ed558ccdL;
        x = (x ^ (x >>> 33)) * 0xc4ceb9fe1a85ec53L;
        x = x ^ (x >>> 33);
        return x;
    }

    public static long randomSeed() {
        return random.nextLong();
    }

    /**
     * Shrink the hash to a value 0..n. Kind of like modulo, but using
     * multiplication and shift, which are faster to compute.
     *
     * @param hash the hash
     * @param n the maximum of the result
     * @return the reduced value
     */
    public static int reduce(int hash, int n) {
        // http://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
        return (int) (((hash & 0xffffffffL) * n) >>> 32);
    }

    public static byte[]  Get_SHA_256(byte[] passwordToHash) {
        try {
            MessageDigest md = MessageDigest.getInstance("Sha-256");
            md.update(passwordToHash);
            byte[] bytes = md.digest();
            return bytes;
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static byte[]  Get_SHA_128(byte[] passwordToHash) {
        try {
            MessageDigest md = MessageDigest.getInstance("Sha-256");
            md.update(passwordToHash);
            byte[] bytes = md.digest();
            byte[] hash_128 = new byte[16];
            System.arraycopy(bytes,0,hash_128,0,16);
            return hash_128;
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }
}

package utils;
import java.util.Random;

/**
 * The xor filter, a new algorithm that can replace a Bloom filter.
 *
 * It needs 1.23 log(1/fpp) bits per key. It is related to the BDZ algorithm [1]
 * (a minimal perfect hash function algorithm).
 *
 * [1] paper: Simple and Space-Efficient Minimal Perfect Hash Functions -
 * http://cmph.sourceforge.net/papers/wads07.pdf
 */
public class Xor8 {

    private static final int BITS_PER_FINGERPRINT = 8;
    private static final int HASHES = 3;
    private static final int OFFSET = 2;
    private static final int FACTOR_TIMES_100 = 123;
    private final int size;
    private final int arrayLength;
    private final int blockLength;
    private long seed;
    private byte[][] ciphertext;
    private final int bitCount;
    private static Random random = new Random();


    private static int getArrayLength(int size) {
        return (int) (OFFSET + (long) FACTOR_TIMES_100 * size / 100);
    }


    public Xor8(long[] keys, byte[][] ct) {
        this.size = keys.length;
        arrayLength = getArrayLength(size);
        bitCount = arrayLength * BITS_PER_FINGERPRINT;
        blockLength = arrayLength / HASHES;
        int m = arrayLength;
        ciphertext = new byte[m][];
        long[] reverseOrder = new long[arrayLength];
        byte[] reverseH = new byte[arrayLength];
        int reverseOrderPos;
        long seed;
        do {
            seed = Hash.randomSeed();
            byte[] t2count = new byte[m];
            long[] t2 = new long[m];
            for (int i = 0; i < size; i++) {
                long k = i;
                for (int hi = 0; hi < HASHES; hi++) {
                    int h = getHash(keys[i], seed, hi);
                    t2[h] ^= k;
                    if (t2count[h] > 120) {
                        // probably something wrong with the hash function
                        // let us not crash the system:
                        throw new IllegalArgumentException();
                    }
                    t2count[h]++;
                }
            }
            reverseOrderPos = 0;
            int[][] alone = new int[HASHES][blockLength];
            int[] alonePos = new int[HASHES];
            for (int nextAlone = 0; nextAlone < HASHES; nextAlone++) {
                for (int i = 0; i < blockLength; i++) {
                    if (t2count[nextAlone * blockLength + i] == 1) {
                        alone[nextAlone][alonePos[nextAlone]++] = nextAlone * blockLength + i;
                    }
                }
            }
            int found = -1;
            while (true) {
                int i = -1;
                for (int hi = 0; hi < HASHES; hi++) {
                    if (alonePos[hi] > 0) {
                        i = alone[hi][--alonePos[hi]];
                        found = hi;
                        break;
                    }
                }
                if (i == -1) {
                    // no entry found
                    break;
                }
                if (t2count[i] <= 0) {
                    continue;
                }
                long k = t2[i];

                if (t2count[i] != 1) {
                    throw new AssertionError();
                }
                --t2count[i];
                for (int hi = 0; hi < HASHES; hi++) {
                    if (hi != found) {
                        int h = getHash(keys[(int)k], seed, hi);
                        int newCount = --t2count[h];
                        if (newCount == 1) {
                            alone[hi][alonePos[hi]++] = h;
                        }
                        t2[h] ^= k;
                    }
                }
                reverseOrder[reverseOrderPos] = k;
                reverseH[reverseOrderPos] = (byte) found; // i
                reverseOrderPos++;
            }
        } while (reverseOrderPos != size);
        this.seed = seed;
        for (int i = reverseOrderPos - 1; i >= 0; i--) {
            int k = (int)reverseOrder[i];
            int found = reverseH[i];
            int change = -1;
            byte[] xor = ct[k];
            for (int hi = 0; hi < HASHES; hi++) {
                int h = getHash(keys[k], seed, hi);
                if (found == hi) {
                    change = h;
                } else {
                    if (ciphertext[h] == null){
                        ciphertext[h] = Hash.Get_SHA_256(tool.longToBytes(random.nextInt(10000)));
                    }
                    xor = tool.Xor(xor, ciphertext[h]);
                }
            }
            ciphertext[change] = xor;
        }
        for (int i = 0; i < ciphertext.length; i++) {
            if (ciphertext[i] == null){
                ciphertext[i] = Hash.Get_SHA_256(tool.longToBytes(random.nextInt(10000)));
            }
        }
    }
    public byte[] search(long key) {
        long hash = Hash.hash64(key, seed); // x
        int f = fingerprint(hash);
        int[] r = new int[3];
        int[] h = new int[3];
        r[0] = (int) hash;
        r[1] = (int) Long.rotateLeft(hash, 21);
        r[2] = (int) Long.rotateLeft(hash, 42);
        for (int i = 0; i < 3; i++) {
            h[i] = Hash.reduce(r[i], blockLength) + i * blockLength;
        }
        return tool.Xor(ciphertext[h[0]], tool.Xor(ciphertext[h[1]], ciphertext[h[2]]));
    }

    private int getHash(long key, long seed, int index) {
        long r = Long.rotateLeft(Hash.hash64(key, seed), 21 * index);
        r = Hash.reduce((int) r, blockLength);
        r = r + index * blockLength;
        return (int) r;
    }

    private int fingerprint(long hash) {
        return (int) (hash & ((1 << BITS_PER_FINGERPRINT) - 1));
    }

    public byte[][] getCiphertext() {
        return ciphertext;
    }
}

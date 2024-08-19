package server;

import client.Setup_JXTpp;
import utils.Bloom;
import utils.Xor8;
import utils.tool;

import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: 杜凯
 * @Date: 2023/11/06/19:09
 * @Description: the server's process about the search algorithm of JXT++
 */
public class Server_JXTpp {
    private Map<BigInteger, ArrayList<byte[]>> tset;
    private Bloom[] f;
    private Xor8[] xor;
    private BigInteger stag1;

    public Server_JXTpp(Map<BigInteger, ArrayList<byte[]>> tset, Setup_JXTpp[] table, BigInteger stag) {
        int table_num = table.length;
        this.tset = tset;
        this.stag1 = stag;
        f = new Bloom[table_num];
        xor = new Xor8[table_num];
        for (int i = 0; i < table_num; i++) {
            f[i] = table[i].getF();
            xor[i] = table[i].getXor();
        }
    }

    /**
     * the server operations of the search algorithm for JXT++
     * @param join_tokens jointokens
     * @return the matching results
     */
    public ArrayList<byte[][]> search(ArrayList<byte[][]> join_tokens) {
        ArrayList<byte[][]> res = new ArrayList<>();
        ArrayList<byte[]> token1_tset = tset.get(stag1);

        for (int i = 0; i < token1_tset.size(); i++) {
            byte[] token1 = token1_tset.get(i);
            long[] xtoken1_long = new long[join_tokens.size()];
            for (int j = 1; j < join_tokens.size(); j++) {
                byte[] xtoken1 = tool.Xor(token1, join_tokens.get(j)[0]);
                xtoken1_long[j] = tool.bytesToLong(xtoken1);
                if (!f[j].mayContain(xtoken1_long[j])) {
                    break;
                }
                if (j == join_tokens.size() - 1) {
                    xtoken1_long[0] = tool.bytesToLong(tool.Xor(token1, join_tokens.get(0)[0]));
                    for (int k = 0; k < join_tokens.size(); k++) {
                        byte[][] join_token = join_tokens.get(k);
                        byte[][] ct = new byte[join_token.length][];
                        for (int h = 0; h < join_token.length; h++) {
                            long xtoken;
                            if (h == 0) xtoken = xtoken1_long[k];
                            xtoken = tool.bytesToLong(tool.Xor(token1, join_token[h]));
                            ct[h] = xor[k].search(xtoken);
                        }
                        res.add(ct);
                    }
                }
            }
        }
        return res;
    }
}

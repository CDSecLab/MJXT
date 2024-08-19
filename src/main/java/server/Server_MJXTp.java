package server;

import client.Setup_JXTp;
import utils.Bloom;
import utils.tool;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: 杜凯
 * @Date: 2023/09/27/19:54
 * @Description: the server's process about the search algorithm of JXT called MJXT+, where MJXT+ is a Join-query scheme
 * supporting the multi-tables (three or more) search by a trivial method which leak SRP(sub-query pattern).
 */
public class Server_MJXTp {
    private Map<BigInteger, ArrayList<byte[]>> tset;
    private Bloom[] f;
    private Map<Long, ArrayList<byte[]>>[] cset;
    private BigInteger stag1;

    public Server_MJXTp(Map<BigInteger, ArrayList<byte[]>> tset, Setup_JXTp[] table) {
        int table_num = table.length;
        this.tset = tset;
        f = new Bloom[table_num];
        cset = new Map[table_num];
        for (int i = 0; i < table_num; i++) {
            f[i] = table[i].getF();
            cset[i] = table[i].getCset();
        }
    }
    /**
     * Get the number of the first table's TSet entries
     * @param stag1 the first table's stag
     * @return the number of the matching TSet entries
     */
    public int tset_table1_cnt(BigInteger stag1) {
        this.stag1 = stag1;
        return tset.get(stag1).size();
    }

    /**
     * the server operations of the search algorithm for MJXT+
     * @param join_tokens the jointokens for MJXT+
     * @return the matching result
     */
    public ArrayList<ArrayList<byte[]>> search(ArrayList<byte[][]> join_tokens) {
        ArrayList<ArrayList<byte[]>> res = new ArrayList<>();
        ArrayList<byte[]> token1_tset = tset.get(stag1);
        for (int i = 0; i < token1_tset.size(); i++) {
            byte[] token1 = token1_tset.get(i);
            long[] xtoken_long = new long[join_tokens.size()];
            for (int j = 1; j < join_tokens.size(); j++) {
                byte[] xtoken = tool.Xor(token1, join_tokens.get(j)[i]);
                xtoken_long[j] = tool.bytesToLong(xtoken);
                if (!f[j].mayContain(xtoken_long[j])) break;
                if (j == join_tokens.size() - 1) {
                    xtoken_long[0] = tool.bytesToLong(tool.Xor(token1, join_tokens.get(0)[i]));
                    for (int k = 0; k < xtoken_long.length; k++) {
                        ArrayList<byte[]> ct = cset[k].get(xtoken_long[k]);
                        res.add(ct);
                    }
                }
            }
        }
        return res;
    }
}

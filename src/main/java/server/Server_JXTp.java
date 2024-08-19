package server;

import utils.Bloom;
import utils.tool;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: 杜凯
 * @Date: 2023/09/22/20:40
 * @Description: the server's process about the search algorithm of JXT+
 */
public class Server_JXTp {
    private Map<BigInteger, ArrayList<byte[]>> tset;
    private Bloom f_1;
    private Bloom f_2;
    private Map<Long, ArrayList<byte[]>> cset1;
    private Map<Long, ArrayList<byte[]>> cset2;
    private BigInteger stag1;
    public Server_JXTp(Map<BigInteger, ArrayList<byte[]>> tset, Bloom f_1, Map<Long, ArrayList<byte[]>> cset1,
                       Bloom f_2, Map<Long, ArrayList<byte[]>> cset2){
        this.tset = tset;
        this.f_1 = f_1;
        this.cset1 = cset1;
        this.f_2 = f_2;
        this.cset2 = cset2;
    }

    /**
     * Get the number of the first table's TSet entries
     * @param stag1 the first table's stag
     * @return the number of the matching TSet entries
     */
    public int tset_table1_cnt(BigInteger stag1){
        this.stag1 = stag1;
        return tset.get(stag1).size();
    }

    /**
     * the server operations of the search algorithm for JXT+
     * @param stoken sjointoken in JXT+
     * @param xtoken xjointoken in JXT+
     * @return the matching results
     */
    public ArrayList<ArrayList<byte[]>> search(byte[][] stoken, byte[][] xtoken){
        ArrayList<ArrayList<byte[]>> res = new ArrayList<>();
        
        ArrayList<byte[]> token1_tset = tset.get(stag1);
        for (int i = 0; i < token1_tset.size(); i++) {
            byte[] xtoken_t = tool.Xor(xtoken[i], token1_tset.get(i));
            long xtoken_long = tool.bytesToLong(xtoken_t);
            if (f_2.mayContain(xtoken_long)){
                byte[] stoken_t = tool.Xor(stoken[i], token1_tset.get(i));
                res.add(cset1.get(tool.bytesToLong(stoken_t)));
                res.add(cset2.get(xtoken_long));
            }
        }
        return res;
    }
}

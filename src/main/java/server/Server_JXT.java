package server;

import utils.Bloom;
import utils.tool;
import utils.tuple;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: 杜凯
 * @Date: 2023/09/14/16:58
 * @Description: the server's process about the search algorithm of JXT
 */
public class Server_JXT {
    private Map<BigInteger, ArrayList<tuple>> tset;

    private Bloom f_1;
    private Bloom f_2;

    private BigInteger stag1;
    private BigInteger stag2;
    public Server_JXT(Map<BigInteger, ArrayList<tuple>> tset, Bloom f_1, Bloom f_2){
        this.tset = tset;
        this.f_1 = f_1;
        this.f_2 = f_2;
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
     * Get the number of the second table's TSet entries
     * @param stag2 the second table's stag
     * @return the number of the matching TSet entries
     */
    public int tset_table2_cnt(BigInteger stag2){
        this.stag2 = stag2;
        return tset.get(stag2).size();
    }

    /**
     * the server operations of the search algorithm for JXT
     * @param token1 joinToken(1) in JXT
     * @param token2 joinToken(1) in JXT
     * @param join_column the chosen column of the join attribute
     * @return the matching result
     */
    public ArrayList<byte[]> search(byte[][] token1, byte[][] token2, int join_column){
        ArrayList<byte[]> res = new ArrayList<>();

        ArrayList<tuple> token1_tset = tset.get(stag1);
        ArrayList<tuple> token2_tset = tset.get(stag2);
        byte[][] xtoken1 = new byte[token1_tset.size()][];
        byte[][] xtoken2 = new byte[token2_tset.size()][];
        for (int i = 0; i < token1_tset.size(); i++) {
            xtoken1[i] = tool.Xor(token1_tset.get(i).h_y[join_column], token1[i]);
        }
        for (int i = 0; i < token2_tset.size(); i++) {
            xtoken2[i] = tool.Xor(token2_tset.get(i).h_x[join_column], token2[i]);
        }

        for (int i = 0; i < token1_tset.size(); i++) {
            for (int j = 0; j < token2_tset.size(); j++) {
                long x = tool.bytesToLong(tool.Xor(xtoken1[i], xtoken2[j]));
                if (f_2.mayContain(x)){//if contains, the records satisfy the query
                    res.add(token1_tset.get(i).ct);
                    res.add(token2_tset.get(j).ct);
                }
            }
        }
        return res;
    }
}

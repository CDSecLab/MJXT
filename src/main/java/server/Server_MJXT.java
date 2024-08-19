package server;

import client.Setup_JXT;
import utils.Bloom;
import utils.tool;
import utils.tuple;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: 杜凯
 * @Date: 2024/03/20/9:24
 * @Description: the server's process about the search algorithm of JXT called MJXT, where MJXT is a Join-query scheme
 * supporting the multi-tables (three or more) search by a trivial method which leak SRP(sub-query pattern).
 */
public class Server_MJXT {
    private Map<BigInteger, ArrayList<tuple>> tset;
    private Bloom[] f;
    private BigInteger[] stag;
    private int table_num;

    public Server_MJXT(Map<BigInteger, ArrayList<tuple>> tset, Setup_JXT[] table){
        this.tset = tset;
        table_num = table.length;
        f = new Bloom[table_num];
        for (int i = 0; i < table_num; i++) {
            f[i] = table[i].getF();
        }
    }
    /**
    * Get the number of the both table's TSet entries
    * @param stag the both table's stag
    * @return the numbers of the matching TSet entries
     */
    public int[] tset_table_cnt(BigInteger[] stag){
        this.stag = stag;
        int[] cnt = new int[table_num];
        for (int i = 0; i < table_num; i++) {
            cnt[i] = tset.get(stag[i]).size();
        }
        return cnt;
    }

    /**
     * the server operations of the search algorithm for MJXT
     * @param join_tokens1 joinToken(1) in JXT
     * @param join_tokens2 joinToken(1) in JXT
     * @param join_column the chosen column of the join attribute
     * @return the matching result (the records of the all tables satisfy the query)
     */
    public ArrayList<ArrayList<byte[]>> search(ArrayList<byte[][]> join_tokens1, ArrayList<byte[][]> join_tokens2, int join_column) {
        ArrayList<ArrayList<byte[]>> res = new ArrayList<>();
        ArrayList<ArrayList<tuple>> tsets = new ArrayList<>();
        for (int i = 0; i < table_num; i++) {
            tsets.add(tset.get(stag[i]));
        }
        ArrayList<tuple> tset_each1 = tsets.get(0);
        Map<tuple, ArrayList<ArrayList<tuple>>> satisfy_tseti = new LinkedHashMap<>();
        for (int i = 1; i < table_num; i++) {
            ArrayList<tuple> tset_eachi = tsets.get(i);
            byte[][] xtoken1 = new byte[tset_each1.size()][];
            byte[][] xtokeni = new byte[tset_eachi.size()][];
            byte[][] join_token_1_i = join_tokens1.get(i - 1);
            byte[][] join_token_i_1 = join_tokens2.get(i - 1);
            for (int j = 0; j < join_token_1_i.length; j++) {
                xtoken1[j] = tool.Xor(tset_each1.get(j).h_y[join_column], join_token_1_i[j]);
            }
            for (int j = 0; j < join_token_i_1.length; j++) {
                xtokeni[j] = tool.Xor(tset_eachi.get(j).h_x[join_column], join_token_i_1[j]);
            }
            for (int j = 0; j < xtoken1.length; j++) {
                int counter = 0;
                for (int k = 0; k < xtokeni.length; k++) {
                    long x = tool.bytesToLong(tool.Xor(xtoken1[j], xtokeni[k]));
                    if (!f[i].mayContain(x)){
                        counter++;
                    }else{
                        if (!satisfy_tseti.containsKey(tset_each1.get(j)) && i == 1){
                            ArrayList<ArrayList<tuple>> tablei = new ArrayList<>();
                            ArrayList<tuple> tablei_tset = new ArrayList<>();
                            tablei_tset.add(tset_eachi.get(k));
                            tablei.add(tablei_tset);
                            satisfy_tseti.put(tset_each1.get(j), tablei);
                        }else {
                            ArrayList<ArrayList<tuple>> tablei = satisfy_tseti.get(tset_each1.get(j));
                            ArrayList<tuple> tablei_tset = new ArrayList<>();
                            tablei_tset.add(tset_eachi.get(k));
                            tablei.add(tablei_tset);
                            satisfy_tseti.put(tset_each1.get(j), tablei);
                        }
                    }
                    if (counter == xtokeni.length){
                        satisfy_tseti.remove(tset_each1.get(j));
                        counter = 0;
                    }
                }
            }
        }
        for (int i = 0; i < table_num; i++) {
            ArrayList<byte[]> res_i = new ArrayList<>();
            res.add(res_i);
        }
        for (Map.Entry<tuple, ArrayList<ArrayList<tuple>>> entry : satisfy_tseti.entrySet()){
            res.get(0).add(entry.getKey().ct);
            ArrayList<ArrayList<tuple>> res_tset = entry.getValue();
            for (int i = 1; i < table_num; i++) {
                for (int j = 0; j < res_tset.size() / (table_num - 1); j++) {
                    ArrayList<tuple> res_i = res_tset.get((i - 1) * (table_num - 1) + j);
                    for (int k = 0; k < res_i.size(); k++) {
                        byte[] ct = res_i.get(k).ct;
                        if (!res.get(i).contains(ct)){
                            res.get(i).add(ct);
                        }
                    }
                }
            }
        }
        return res;
    }
}

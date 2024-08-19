import client.Setup_JXTpp;
import server.Server_JXTpp;
import utils.AESUtil;
import utils.Hash;
import utils.tool;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This java file aims to run the JXT++ algorithm.
 * This file can generate the Figure 5 in the paper of JXT++ by setting different table_num.
 * @Author: 杜凯
 * @Date: 2023/11/06/17:56
 * @Description: JXT++
 */
public class JXTpp {
    private static String K_token = "89b7a92966f6eb32";
    private static String K_w = "7975922666f6eb02";
    private static String K_wp = "787599ac86f2e82";
    private static String K_h = "9874a22554e7db85";
    private static String K_aes = "8975924566f6e252";
    private static String K_c = "6574b33984e7fb55";
    public static void main(String[] args) {
        //Step 1 prepare the parameters
        //the number of attributes which aren't the join attribute in the table.
        int key_colnum = 9;//the values{5, 6, 7, 8, 9} for our dataset
        //the number join attributes in the table.
        int join_column = 1;//the values{5, 4, 3, 2, 1} for our dataset
        int record_num = (int)Math.pow(2, 16);//the number of records of the table (65536 lines)
        int table_num = 2;//the number of the queried tables
        String condition = "";//used to choose different tables from dataset, e.g., "_4".
        String[] keyword = new String[table_num];
        String[] join_attr = new String[table_num];
        //Step 2 begin to set up
        System.out.println("------------- JXT++ ---------------");
        System.out.println("---------- JXT++ setup ------------");
        long setup_start = System.nanoTime();
        Setup_JXTpp[] table = new Setup_JXTpp[table_num];
        Map<BigInteger, ArrayList<byte[]>> tset = new LinkedHashMap<>();
        int[][] l_max = new int[table_num][join_column];
        for (int i = 0; i < table_num; i++) {
            table[i] = new Setup_JXTpp(i + 1, key_colnum, join_column, record_num, condition);
            table[i].construct();
            tset.putAll(table[i].getTset());
            System.arraycopy(table[i].getL_max(), 0, l_max[i], 0, join_column);
        }
        long setup_end = System.nanoTime();
        System.out.println("JXT++ setup time : " + (setup_end - setup_start)/Math.pow(10, 6) + " ms");

        //Step 3 begin to search
        System.out.println("---------- JXT++ search -----------");
        for (int i = 0; i < table_num; i++) {//set the queried attribute-value pairs and join attributes
            keyword[i] = "keyword0" + "table" + (i + 1) + "_keyword_0_0";
            join_attr[i] = "join-attr0";
        }

        long search_all = 0;
        for (int x = 0; x < 1000; x++) {//run 1000 times
            long search_start = System.nanoTime();
            //Step 3.1 compute the stag
            BigInteger stag1 = new BigInteger(Hash.Get_SHA_256((K_token + keyword[0] + join_attr[0] + 1).getBytes(StandardCharsets.UTF_8)));
            Server_JXTpp serverMMJXTpp = new Server_JXTpp(tset, table, stag1);
            byte[] w_0 = Hash.Get_SHA_256((K_w + keyword[0]).getBytes(StandardCharsets.UTF_8));
            Map<Integer, byte[]> map_cnt = new LinkedHashMap<>();
            ArrayList<byte[][]> join_tokens = new ArrayList<>();
            for (int i = 0; i < table_num; i++) {//Step 3.2 compute the joinTokens
                byte[] w_i = Hash.Get_SHA_256((K_wp + keyword[i]).getBytes(StandardCharsets.UTF_8));
                byte[] join_hash = Hash.Get_SHA_256((K_h + join_attr[i]).getBytes(StandardCharsets.UTF_8));
                byte[][] join_token = new byte[l_max[i][0]][];
                for (int j = 1; j <= l_max[i][0]; j++) {
                    byte[] cnt;
                    if (map_cnt.containsKey(j)){
                        cnt = map_cnt.get(j);
                    }else {
                        cnt = Hash.Get_SHA_256((K_c + j).getBytes(StandardCharsets.UTF_8));
                        map_cnt.put(j, cnt);
                    }
                    join_token[j - 1] = tool.Xor(tool.Xor(tool.Xor(w_0, w_i), join_hash), cnt);
                }
                join_tokens.add(join_token);
            }
            //Step 3.3 the server returns the satisfying results
            ArrayList<byte[][]> res = serverMMJXTpp.search(join_tokens);
            //Step 3.4 decrypt the encrypted identifiers
            byte[][] k_dec = new byte[table_num][];
            for (int i = 0; i < table_num; i++) {
                k_dec[i] = Hash.Get_SHA_256((K_aes + keyword[i]).getBytes());
            }
            int res_size = 0;
            for (int i = 0; i < res.size(); i++) {
                byte[][] res_i = res.get(i);
                for (int j = 0; j < res_i.length; j++) {
                    byte[] id = AESUtil.decrypt(k_dec[i % table_num], res_i[j]);
                    if (id != null){
                        if (x == 0) {
                            System.out.print(new String(id) + ",");
                            if ((j + 1) % res_i.length == 0) System.out.println();
                        }
                        res_size++;
                    }
                }
            }
            long search_end = System.nanoTime();
            search_all += search_end - search_start;
            if (x == 0) {
                System.out.println("res size : " + res_size);
            }
        }
        //compute the average the search time cost, note that the average of 1000 times
        System.out.println("JXT++ average search time : " + search_all / Math.pow(10, 6 + 3) + " ms");
    }
}

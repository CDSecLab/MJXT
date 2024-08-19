import client.Setup_JXT;
import server.Server_MJXT;
import utils.AESUtil;
import utils.Hash;
import utils.tool;
import utils.tuple;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This java file aime to extend JXT to support the search of multi-tables(three or more) by the trivial method.
 * This file can generate the Figure 5 in the paper of MJXT by setting different table_num.
 * @Author: 杜凯
 * @Date: 2024/03/20/9:23
 * @Description: JXT (multi-tables version)
 */
public class M_JXT {
    private static String K_token = "89b7a92966f6eb32";
    private static String K_h1 = "9874a22554e7db85";
    private static String K_h2 = "fad78974156f6b25";
    private static String K_aes = "8975924566f6e252";

    public static void main(String[] args) {
        //Step 1 prepare the parameters
        //the number of attributes which aren't the join attribute in the table.
        int key_colnum = 9;//the values{5, 6, 7, 8, 9} for our dataset
        //the number join attributes in the table.
        int join_column = 1;//the values{5, 4, 3, 2, 1} for our dataset
        int record_num = (int)Math.pow(2, 16);//the number of records of the table (65536 lines)
        int table_num = 3;//the number of the queried tables
        String condition = "";//used to choose different tables from dataset, e.g., "_4".
        String[] keyword = new String[table_num];
        String[] join_attr = new String[table_num];
        //Step 2 begin to set up
        System.out.println("------------- MJXT ---------------");
        System.out.println("----------- MJXT setup -----------");
        long setup_start = System.nanoTime();
        Setup_JXT[] table = new Setup_JXT[table_num];
        Map<BigInteger, ArrayList<tuple>> tset = new LinkedHashMap<>();
        for (int i = 0; i < table_num; i++) {
            table[i] = new Setup_JXT(i + 1, key_colnum, join_column, record_num, condition);
            table[i].construct();
            tset.putAll(table[i].getTset());
        }
        long setup_end = System.nanoTime();
        System.out.println("MJXT setup time : " + (setup_end - setup_start)/Math.pow(10, 6) + " ms");

        //Step 3 begin to search
        System.out.println("------------ MJXT search ------------");
        for (int i = 0; i < table_num; i++) {//set the queried attribute-value pairs and join attributes
            keyword[i] = "keyword0" + "table" + (i + 1) + "_keyword_0_0";
            join_attr[i] = "join-attr0";
        }
        long search_all = 0;
        for (int x = 0; x < 1000; x++) {//run 1000 times
            long search_start = System.nanoTime();
            Server_MJXT serverMJXT = new Server_MJXT(tset, table);
            //Step 3.1 compute the stags
            BigInteger[] stag = new BigInteger[table_num];
            for (int i = 0; i < table_num; i++) {
                stag[i] = new BigInteger(Hash.Get_SHA_256((K_token + keyword[i] + (i + 1)).getBytes(StandardCharsets.UTF_8)));
            }
            int[] cnt = serverMJXT.tset_table_cnt(stag);
            byte[][] keyword_0 = new byte[table_num][];
            keyword_0[0] = Hash.Get_SHA_256((K_h2 + keyword[0] + 0).getBytes(StandardCharsets.UTF_8));//第一张表为驱动表
            for (int i = 1; i < table_num; i++) {
                keyword_0[i] = Hash.Get_SHA_256((K_h1 + keyword[i] + 0).getBytes(StandardCharsets.UTF_8));
            }
            //Step 3.2 compute the joinTokens
            ArrayList<byte[][]> join_tokens1 = new ArrayList<>();
            for (int i = 1; i < table_num; i++) {
                byte[][] join_token = new byte[cnt[0]][];
                for (int j = 0; j < cnt[0]; j++) {
                    join_token[j] = tool.Xor(Hash.Get_SHA_256((K_h2 + keyword[0] + (j + 1)).getBytes(StandardCharsets.UTF_8)), keyword_0[i]);
                }
                join_tokens1.add(join_token);
            }
            ArrayList<byte[][]> join_tokens2 = new ArrayList<>();
            for (int i = 1; i < table_num; i++) {
                byte[][] join_token = new byte[cnt[i]][];
                for (int j = 0; j < cnt[i]; j++) {
                    join_token[j] = tool.Xor(Hash.Get_SHA_256((K_h1 + keyword[i] + (j + 1)).getBytes(StandardCharsets.UTF_8)), keyword_0[0]);
                }
                join_tokens2.add(join_token);
            }
            //Step 3.3 the server returns the satisfying results
            ArrayList<ArrayList<byte[]>> res = serverMJXT.search(join_tokens1, join_tokens2, 0);
            //Step 3.4 decrypt the encrypted identifiers
            byte[][] k_dec = new byte[table_num][];
            for (int i = 0; i < table_num; i++) {
                k_dec[i] = Hash.Get_SHA_256((K_aes + keyword[i]).getBytes());
            }
            int res_size = 0;
            for (int i = 0; i < res.size(); i++) {
                ArrayList<byte[]> ct = res.get(i);
                for (int j = 0; j < ct.size(); j++) {
                    byte[] id = AESUtil.decrypt(k_dec[i % table_num], ct.get(j));
                    if (x == 0) {
                        System.out.print(new String(id) + ",");
                    }
                    res_size++;
                }
                if (x == 0) System.out.println();
            }
            long search_end = System.nanoTime();
            search_all += search_end - search_start;
            if (x == 0) {
                System.out.println("res size : " + res_size);
            }
        }
        //compute the average the search time cost, note that the average of 1000 times
        System.out.println("MJXT average search time : " + search_all / Math.pow(10, 6 + 3) + " ms");
    }
}

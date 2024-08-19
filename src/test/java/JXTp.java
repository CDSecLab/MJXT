import client.Setup_JXTp;
import server.Server_JXTp;
import utils.AESUtil;
import utils.Hash;
import utils.tool;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;

/**
 * This java file aims to run the JXT+ algorithm.
 *
 * @Author: 杜凯
 * @Date: 2023/09/08/14:36
 * @Description: JXT+
 */
public class JXTp {
    private static String K_token = "89b7a92966f6eb32";
    private static String K_w = "7975922666f6eb02";
    private static String K_h = "9874a22554e7db85";
    private static String K_aes = "8975924566f6e252";

    public static void main(String[] args) {
        //Step 1 prepare the parameters
        //the number of attributes which aren't the join attribute in the table.
        int key_colnum = 9;//the values{5, 6, 7, 8, 9} for our dataset
        //the number join attributes in the table.
        int join_column = 1;//the values{5, 4, 3, 2, 1} for our dataset
        int record_num = (int)Math.pow(2, 16);//the number of records of the table (65536 lines)
        String condition = "";//used to choose different tables from dataset, e.g., "_4".

        System.out.println("------------- JXT+ ---------------");
        System.out.println("------------ JXT+ setup ------------");
        //Step 2 begin to set up
        long setup_start = System.nanoTime();
        Setup_JXTp table_1 = new Setup_JXTp(1, key_colnum, join_column, record_num, condition);
        table_1.construct();
        Map<BigInteger, ArrayList<byte[]>> tset = table_1.getTset();

        Setup_JXTp table_2 = new Setup_JXTp(2, key_colnum, join_column, record_num, condition);
        table_2.construct();
        tset.putAll(table_2.getTset());
        long setup_end = System.nanoTime();
        System.out.println("JXT+ setup time : " + (setup_end - setup_start)/Math.pow(10, 6) + " ms");
        //Step 3 begin to search
        System.out.println("------------ JXT+ search ------------");
        String keyword1 = "keyword0" + "table1_keyword_0_0";//queried attribute-value pair w1
        String keyword2 = "keyword0" + "table2_keyword_0_0";//queried attribute-value pair w2
        //note that for JXT+ supporting equi-join, join attributes can own different names
        String join_attr1 = "join-attr0";//chosen join attribute attr_1
        String join_attr2 = "join-attr0";//chosen join attribute attr_2
        long search_all = 0;
        for (int x = 0; x < 1000; x++) {//run 1000 times
            long search_start = System.nanoTime();
            Server_JXTp serverMJXT = new Server_JXTp(tset, table_1.getF(),
                    table_1.getCset(), table_2.getF(), table_2.getCset());
            //Step 3.1 compute the stag
            BigInteger stag1 = new BigInteger(Hash.Get_SHA_256((K_token + keyword1 + join_attr1 + 1).getBytes(StandardCharsets.UTF_8)));
            int cnt1 = serverMJXT.tset_table1_cnt(stag1);
            byte[][] join_token1 = new byte[cnt1][];
            byte[][] join_token2 = new byte[cnt1][];
            byte[] w = Hash.Get_SHA_256((K_w + keyword1 + 0).getBytes(StandardCharsets.UTF_8));
            byte[] y = Hash.Get_SHA_256((K_w + keyword2 + 0).getBytes(StandardCharsets.UTF_8));
            byte[] join_hash1 = Hash.Get_SHA_256((K_h + join_attr1).getBytes(StandardCharsets.UTF_8));
            byte[] join_hash2 = Hash.Get_SHA_256((K_h + join_attr2).getBytes(StandardCharsets.UTF_8));
            //Step 3.2 compute the joinTokens
            for (int i = 0; i < cnt1; i++) {
                byte[] w_cnt = Hash.Get_SHA_256((K_w + keyword1 + (i + 1)).getBytes(StandardCharsets.UTF_8));
                join_token1[i] = tool.Xor(tool.Xor(w, w_cnt), join_hash1);
                join_token2[i] = tool.Xor(tool.Xor(y, w_cnt), join_hash2);
            }
            //Step 3.3 the server returns the satisfying results
            ArrayList<ArrayList<byte[]>> res = serverMJXT.search(join_token1, join_token2);
            byte[] k_dec1 = Hash.Get_SHA_256((K_aes + keyword1).getBytes());
            byte[] k_dec2 = Hash.Get_SHA_256((K_aes + keyword2).getBytes());
            //Step 3.4 decrypt the encrypted identifiers
            for (int i = 0; i < res.size(); i++) {
                if (i % 2 == 0){
                    ArrayList<byte[]> res_i = res.get(i);
                    for (int j = 0; j < res_i.size(); j++) {
                        if (x == 0) System.out.println(new String(AESUtil.decrypt(k_dec1, res_i.get(j))) + ",");
                        else AESUtil.decrypt(k_dec1, res_i.get(j));
                    }
                }else {
                    ArrayList<byte[]> res_i = res.get(i);
                    for (int j = 0; j < res_i.size(); j++) {
                        if (x == 0) System.out.println(new String(AESUtil.decrypt(k_dec2, res_i.get(j))) + ",");
                        else AESUtil.decrypt(k_dec2, res_i.get(j));
                    }
                }
            }
            long search_end = System.nanoTime();
            search_all += search_end - search_start;
            if (x == 0) {
                System.out.println("res size : " + res.size());
            }
        }
        //compute the average the search time cost, note that the average of 1000 times
        System.out.println("JXT+ average search time : " + search_all / Math.pow(10, 6 + 3) + " ms");
    }
}

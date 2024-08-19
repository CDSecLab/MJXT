import client.Setup_JXT;
import server.Server_JXT;
import utils.AESUtil;

import utils.Hash;
import utils.tool;
import utils.tuple;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;


/**
 * This java file aims to test JXT's query efficiency for the table where 20 attribute-value pairs are queried in batches.
 * This code can generate the data about JXT for Figure 2 and Figure 3 in the paper.
 *
 * @Author: 杜凯
 * @Date: 2023/09/12/15:23
 * @Description:
 */
public class test_JXT {

    private static String K_token = "89b7a92966f6eb32";
    private static String K_h1 = "9874a22554e7db85";
    private static String K_h2 = "fad78974156f6b25";
    private static String K_aes = "8975924566f6e252";

    public static void main(String[] args) {
        //the number of attributes which aren't the join attribute in the table.
        int key_colnum = 9;
        //the number join attributes in the table.
        int join_column = 1;
        int record_num = (int)Math.pow(2, 16);//the number of records of the table (65536 lines)
        String condition = "";//used to choose different tables from dataset, e.g., "_4".
        System.out.println("--------------------- JXT ------------------------");
        //Step 2 begin to set up
        Setup_JXT table_1 = new Setup_JXT(1, key_colnum, join_column, record_num, condition);
        table_1.construct();
        Map<BigInteger, ArrayList<tuple>> tset = table_1.getTset();
        Setup_JXT table_2 = new Setup_JXT(2, key_colnum, join_column, record_num, condition);
        table_2.construct();
        tset.putAll(table_2.getTset());
        //Step 3 begin to search
        for (int v = 0; v < 20; v++) {
            String keyword1 = "keyword0" + "table1_keyword_" + v + "_0";
            String keyword2 = "keyword0" + "table2_keyword_" + v + "_0";
            System.out.println("----- JXT search(table_keyword_" + v + "_0) -----");
            long search_all = 0;
            for (int x = 0; x < 1000; x++) {//run 1000 times
                long search_start = System.nanoTime();
                Server_JXT serverJXT = new Server_JXT(tset, table_1.getF(),table_2.getF());
                //Step 3.1 compute the stags
                BigInteger stag1 = new BigInteger(Hash.Get_SHA_256((K_token + keyword1 + 1).getBytes(StandardCharsets.UTF_8)));
                BigInteger stag2 = new BigInteger(Hash.Get_SHA_256((K_token + keyword2 + 2).getBytes(StandardCharsets.UTF_8)));
                int cnt1 = serverJXT.tset_table1_cnt(stag1);
                int cnt2 = serverJXT.tset_table2_cnt(stag2);
                byte[][] join_token1 = new byte[cnt1][];
                byte[][] join_token2 = new byte[cnt2][];
                byte[] keyword1_0 = Hash.Get_SHA_256((K_h2 + keyword1 + 0).getBytes(StandardCharsets.UTF_8));
                byte[] keyword2_0 = Hash.Get_SHA_256((K_h1 + keyword2 + 0).getBytes(StandardCharsets.UTF_8));
                //Step 3.2 compute the joinTokens
                for (int i = 0; i < cnt1; i++) {
                    join_token1[i] = tool.Xor(Hash.Get_SHA_256((K_h2 + keyword1 + (i + 1)).getBytes(StandardCharsets.UTF_8)), keyword2_0);
                }
                for (int i = 0; i < cnt2; i++) {
                    join_token2[i] = tool.Xor(Hash.Get_SHA_256((K_h1 + keyword2 + (i + 1)).getBytes(StandardCharsets.UTF_8)), keyword1_0);
                }
                //Step 3.3 the server returns the satisfying results
                ArrayList<byte[]> res = serverJXT.search(join_token1, join_token2, 0);
                //Step 3.4 decrypt the encrypted identifiers
                byte[] k_dec1 = Hash.Get_SHA_256((K_aes + keyword1).getBytes());
                byte[] k_dec2 = Hash.Get_SHA_256((K_aes + keyword2).getBytes());
                for (int i = 0; i < res.size(); i++) {
                    if (i % 2 == 0) AESUtil.decrypt(k_dec1, res.get(i));
                    else AESUtil.decrypt(k_dec2, res.get(i));

                }
                long search_end = System.nanoTime();
                search_all += search_end - search_start;
                if (x == 0) {
                    System.out.println("res size : " + res.size());
                }
            }
            System.out.println("JXT average search time : " + search_all / Math.pow(10, 6 + 3) + " ms");
        }

    }
}

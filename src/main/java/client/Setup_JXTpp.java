package client;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import utils.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: KaiDu
 * @Date: 2023/11/06/16:26
 * @Description: the setup algorithm of JXT++
 */
public class Setup_JXTpp {
    private static String K_aes = "8975924566f6e252";
    private static String K_token = "89b7a92966f6eb32";
    private static String K_w = "7975922666f6eb02";
    private static String K_wp = "787599ac86f2e82";
    private static String K_z = "9862192ad6f6ef65";
    private static String K_h = "9874a22554e7db85";
    private static String K_c = "6574b33984e7fb55";
    private int table_id;
    private int key_column;
    private int join_column;
    private int record_num;
    private String condition;
    private String[] id;
    private String[][] keyword;
    private String[][] join_attr;

    private Bloom f;

    private Xor8 xor;

    private int[] l_max;

    private Map<BigInteger, ArrayList<byte[]>> tset = new LinkedHashMap<>();

    /**
     * @param table_id_ the table index
     * @param key_column_num the number of columns which are not join attribute
     * @param join_column_num the number of column
     * @param record the number of records of the table
     */
    public Setup_JXTpp(int table_id_, int key_column_num, int join_column_num, int record, String condition_t){
        table_id = table_id_;
        key_column = key_column_num;
        join_column = join_column_num;
        record_num = record;
        condition = condition_t;
    }

    public void construct() {
        //Step 1 read the dataset from the tables
        //Step 1.1 prepare the parameters
        l_max = new int[join_column];
        id = new String[record_num + 1];
        keyword = new String[record_num + 1][key_column];
        join_attr = new String[record_num + 1][join_column];
        Map<String, ArrayList<Integer>> reverse_id = new LinkedHashMap<>();
        Map<String, Integer> l_max_cnt = new HashMap<>();
        String path = "data/table" + table_id + "/table" + table_id + "_k" + key_column
                + "_j" + join_column + "_" + record_num + condition + ".csv";
        //Step 1.2 begin to read dataset and statistics the l_max
        try (Reader reader = Files.newBufferedReader(Paths.get(path))) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(reader);
            int counter = 0;
            for (CSVRecord record : records) {
                id[counter] = record.get(0);
                for (int j = 0; j < key_column; j++) {
                    keyword[counter][j] = record.get(j + 1);
                    if (counter != 0) {
                        String kword = keyword[0][j] + record.get(j + 1);//attribute-value pair
                        if (!reverse_id.containsKey(kword))
                            reverse_id.put(kword, new ArrayList<>());
                        reverse_id.get(kword).add(counter);
                        for (int i = 0; i < join_column; i++) {
                            String key = kword + record.get(key_column + i + 1);
                            int cnt;
                            if (l_max_cnt.containsKey(key)) {
                                cnt = l_max_cnt.get(key) + 1;
                                l_max_cnt.put(key, cnt);
                            } else {
                                l_max_cnt.put(key, 1);
                                cnt = 1;
                            }
                            l_max[i] = Math.max(l_max[i], cnt);
                        }
                    }
                }
                //also join-attribute values pairs are also seen as the attribute-value pair
                for (int j = 0; j < join_column; j++) {
                    join_attr[counter][j] = record.get(key_column + j + 1);
                    if (counter != 0) {
                        String kword = join_attr[0][j] + record.get(key_column + j + 1);
                        if (!reverse_id.containsKey(kword))
                            reverse_id.put(kword, new ArrayList<>());
                        reverse_id.get(kword).add(counter);
                        for (int i = 0; i < join_column; i++) {
                            String key = kword + record.get(key_column + i + 1);
                            int cnt;
                            if (l_max_cnt.containsKey(key)) {
                                cnt = l_max_cnt.get(key) + 1;
                                l_max_cnt.put(key, cnt);
                            } else {
                                l_max_cnt.put(key, 1);
                                cnt = 1;
                            }
                            l_max[i] = Math.max(l_max[i], cnt);
                        }
                    }
                }
                counter++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //Step 2 the process of setup
        byte[][] join_hash = new byte[join_column][];
        for (int i = 0; i < join_column; i++) {
            join_hash[i] = Hash.Get_SHA_256((K_h + join_attr[0][i]).getBytes(StandardCharsets.UTF_8));
        }
        Map<Integer, byte[]> cnt_hash = new LinkedHashMap<>();
        long[] xy = new long[record_num * (key_column + join_column) * join_column];
        byte[][] ct = new byte[record_num * (key_column + join_column) * join_column][];
        ArrayList<Long> xset = new ArrayList<>();
        int counter = 0;
        for(String kword : reverse_id.keySet()){
            Map<String, ArrayList<Integer>> join_C = new LinkedHashMap<>();
            int cnt;
            Random random = new Random();
            ArrayList<Integer> reverse_tmp = reverse_id.get(kword);
            byte[] w = Hash.Get_SHA_256((K_wp + kword).getBytes(StandardCharsets.UTF_8));//Z
            byte[] w_0 = Hash.Get_SHA_256((K_w + kword).getBytes(StandardCharsets.UTF_8));//Z'
            ArrayList<ArrayList<byte[]>> stag_map = new ArrayList<>();
            for (int j = 0; j < join_column; j++) {
                stag_map.add(new ArrayList<>());
            }
            byte[] K_enc = Hash.Get_SHA_256((K_aes + kword).getBytes());

            for (int i = 0; i < reverse_tmp.size(); i++) {
                int record_id = reverse_tmp.get(i);
                byte[] ct_tmp = AESUtil.encrypt(K_enc, (id[record_id]).getBytes(StandardCharsets.UTF_8));
                for (int j = 0; j < join_column; j++) {
                    byte[] y = Hash.Get_SHA_256((K_z + join_attr[record_id][j]).getBytes(StandardCharsets.UTF_8));
                    if (join_C.containsKey(join_attr[record_id][j] + join_attr[0][j])){
                        ArrayList<Integer> c_list = join_C.get(join_attr[record_id][j] + join_attr[0][j]);
                        int index = random.nextInt(c_list.size());
                        cnt = c_list.get(index);
                        c_list.remove(index);
                        join_C.replace(join_attr[record_id][j] + join_attr[0][j], c_list);
                    } else{// the pair of join-attribute value and "join attribute" first appears
                        ArrayList<Integer> c_list = new ArrayList<>();
                        for (int k = 0; k < l_max[j]; k++) {
                            c_list.add(k + 1);
                        }
                        int index = random.nextInt(c_list.size());
                        cnt = c_list.get(index);
                        c_list.remove(index);
                        join_C.put(join_attr[record_id][j] + join_attr[0][j], c_list);

                        byte[] xattr = tool.Xor(w_0, y);
                        stag_map.get(j).add(xattr);
                        xset.add(tool.bytesToLong(tool.Xor(tool.Xor(tool.Xor(w, y), join_hash[j]), Hash.Get_SHA_256((K_c + 1).getBytes(StandardCharsets.UTF_8)))));
                    }

                    if (!cnt_hash.containsKey(cnt)){//store the repeated the hash values
                        cnt_hash.put(cnt, Hash.Get_SHA_256((K_c + cnt).getBytes(StandardCharsets.UTF_8)));
                    }

                    xy[counter] = tool.bytesToLong(tool.Xor(tool.Xor(tool.Xor(w, y), join_hash[j]), cnt_hash.get(cnt)));
                    ct[counter] = ct_tmp;
                    counter++;
                }
            }
            for (int i = 0; i < join_column; i++) {
                BigInteger token = new BigInteger(Hash.Get_SHA_256((K_token + kword + join_attr[0][i] + table_id).getBytes(StandardCharsets.UTF_8)));
                tset.put(token, stag_map.get(i));
            }
        }
        long[] xset_array = new long[xset.size()];
        for (int i = 0; i < xset.size(); i++) {
            xset_array[i] = xset.get(i);
        }
        f = Bloom.construct(xset_array, 64);
        xor = new Xor8(xy, ct);
    }

    public Map<BigInteger, ArrayList<byte[]>> getTset() {
        return tset;
    }

    public Bloom getF() {
        return f;
    }

    public Xor8 getXor() {
        return xor;
    }

    public int[] getL_max() {
        return l_max;
    }

    public void Store(String text){
        try {
            FileOutputStream file = new FileOutputStream("data/EDB/JXT++_" + text + ".dat");
            for (BigInteger token : tset.keySet()) {
                file.write(token.toByteArray());
                ArrayList<byte[]> tuples = tset.get(token);
                    for (byte[] t_each : tuples){
                        file.write(t_each);
                    }

            }
            byte[][] xset = f.getData();
            for (int j = 0; j < xset.length; j++) {
                file.write(xset[j]);
            }
            byte[][] cset = xor.getCiphertext();
            for (int j = 0; j < cset.length; j++) {
                file.write(cset[j]);
            }
            file.close();
        } catch (IOException e) {
            System.out.println("Error - " + e.toString());
        }
    }
}

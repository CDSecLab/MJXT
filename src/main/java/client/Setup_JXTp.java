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
 * @Date: 2023/09/22/16:37
 * @Description: the setup algorithm of JXT+
 */
public class Setup_JXTp {
    private static String K_aes = "8975924566f6e252";
    private static String K_token = "89b7a92966f6eb32";
    private static String K_w = "7975922666f6eb02";
    private static String K_z = "9862192ad6f6ef65";
    private static String K_h = "9874a22554e7db85";
    private int table_id;
    private int key_column;
    private int join_column;
    private int record_num;
    private String condition;
    private String[] id;
    private String[][] keyword;
    private String[][] join_attr;
    private Bloom f;//the XSet is implemented by the Bloom filter
    private Map<BigInteger, ArrayList<byte[]>> tset = new LinkedHashMap<>();
    private Map<Long, ArrayList<byte[]>> cset = new HashMap<>();//the CSet for JXT+ is implemented by the map
    /**
     *
     * @param table_id_ the table index
     * @param key_column_num the number of columns which are not join attribute
     * @param join_column_num the number of column
     * @param record the number of records of the table
     */
    public Setup_JXTp(int table_id_, int key_column_num, int join_column_num, int record, String condition_t){
        table_id = table_id_;
        key_column = key_column_num;
        join_column = join_column_num;
        record_num = record;
        condition = condition_t;
    }

    public void construct() {
        //Step 1 read the dataset from the tables
        //Step 1.1 prepare the parameters
        id = new String[record_num + 1];
        keyword = new String[record_num + 1][key_column];
        join_attr = new String[record_num + 1][join_column];
        Map<String, ArrayList<Integer>> reverse_id = new LinkedHashMap<>();//the pairs of (attribute-value pair, record ids)
        String path = "data/table" + table_id + "/table" + table_id + "_k" + key_column
                + "_j" + join_column + "_" + record_num + condition +".csv";
        //Step 1.2 begin to read dataset
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
                    }
                }
                //also join-attribute values pairs are also seen as the attribute-value pair
                for (int j = 0; j < join_column; j++) {
                    join_attr[counter][j] = record.get(key_column + j + 1);
                    if (counter != 0) {
                        String kword = join_attr[0][j] + record.get(key_column + j + 1);//attribute-value pair
                        if (!reverse_id.containsKey(kword))
                            reverse_id.put(kword, new ArrayList<>());
                        reverse_id.get(kword).add(counter);
                    }
                }
                counter++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //Step 2 the process of setup
        byte[][] join_hash = new byte[join_column][];
        for (int i = 0; i < join_column; i++) {//F(K_h, attr_i)
            join_hash[i] = Hash.Get_SHA_256((K_h + join_attr[0][i]).getBytes(StandardCharsets.UTF_8));
        }

        long[] xy = new long[record_num * (key_column + join_column) * join_column];
        int counter = 0;

        for(String kword : reverse_id.keySet()){//for each attribute-value pair
            ArrayList<Integer> reverse_tmp = reverse_id.get(kword);
            byte[] w = Hash.Get_SHA_256((K_w + kword + 0).getBytes(StandardCharsets.UTF_8));//Z_0 = F(w||0)
            ArrayList<ArrayList<byte[]>> t = new ArrayList<>();
            for (int i = 0; i < reverse_tmp.size(); i++) {//for each record
                int record_id = reverse_tmp.get(i);
                byte[] w_cnt = Hash.Get_SHA_256((K_w + kword + (i + 1)).getBytes(StandardCharsets.UTF_8));//Z_cnt
                byte[] K_enc = Hash.Get_SHA_256((K_aes + kword).getBytes());
                byte[] ct_tmp = AESUtil.encrypt(K_enc, id[record_id].getBytes(StandardCharsets.UTF_8));
                for (int j = 0; j < join_column; j++) {//for each join attribute
                    byte[] y = Hash.Get_SHA_256((K_z + join_attr[record_id][j]).getBytes(StandardCharsets.UTF_8));
                    byte[] tset_each = tool.Xor(w_cnt, y);
                    xy[counter] = tool.bytesToLong(tool.Xor(tool.Xor(w, y), join_hash[j]));
                    if (cset.containsKey(xy[counter])){
                        cset.get(xy[counter]).add(ct_tmp);
                        tset_each = tool.Xor(tset_each, K_z.getBytes());
                    }else {
                        ArrayList<byte[]> ct = new ArrayList<>();
                        ct.add(ct_tmp);
                        cset.put(xy[counter], ct);
                    }
                    if (i == 0){
                        t.add(new ArrayList<>());
                    }
                    t.get(j).add(tset_each);
                    counter++;
                }
            }
            for (int i = 0; i < join_column; i++) {//construct the TSet
                BigInteger token = new BigInteger(Hash.Get_SHA_256((K_token + kword + join_attr[0][i] + table_id).getBytes(StandardCharsets.UTF_8)));
                tset.put(token, t.get(i));
            }
        }
        f = Bloom.construct(xy, 64);
    }

    public Map<BigInteger, ArrayList<byte[]>> getTset() {
        return tset;
    }

    public Bloom getF() {
        return f;
    }

    public Map<Long, ArrayList<byte[]>> getCset() {
        return cset;
    }

    public void Store(String text){
        try {
            FileOutputStream file = new FileOutputStream("data/EDB/JXT+_" + text + ".dat");

            for (BigInteger token : tset.keySet()) {
                file.write(token.toByteArray());
                ArrayList<byte[]> tuples = tset.get(token);
                for (byte[] t : tuples) {
                    file.write(t);
                }
            }
            byte[][] xset = f.getData();
            for (int j = 0; j < xset.length; j++) {
                file.write(xset[j]);
            }
            for (Map.Entry<Long, ArrayList<byte[]>> entry : cset.entrySet()){
                file.write(entry.getKey().byteValue());
                ArrayList<byte[]> ct = entry.getValue();
                for (int i = 0; i < ct.size(); i++) {
                    file.write(ct.get(i));
                }
            }
            file.close();
        } catch (IOException e) {
            System.out.println("Error - " + e.toString());
        }
    }
}

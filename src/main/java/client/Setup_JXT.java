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
 * @Date: 2023/09/12/15:44
 * @Description: the setup algorithm of JXT
 */
public class Setup_JXT {
    private static String K_aes = "8975924566f6e252";
    private static String K_token = "89b7a92966f6eb32";
    private static String K_x = "7975922666f6eb02";
    private static String K_y = "9862192ad6f6ef65";
    private static String K_h1 = "9874a22554e7db85";
    private static String K_h2 = "fad78974156f6b25";
    private int table_id;
    private int key_column;
    private int join_column;
    private int record_num;
    private String condition;
    private String[] id;
    private String[][] keyword;
    private String[][] join_attr;
    //the XSet is implemented by the Bloom filter
    private Bloom f;

    private Map<BigInteger, ArrayList<tuple>> tset = new LinkedHashMap<>();

    /**
     *
     * @param table_id_ the table index
     * @param key_column_num the number of columns which are not join attribute
     * @param join_column_num the number of column
     * @param record the number of records of the table
     */
    public Setup_JXT(int table_id_, int key_column_num, int join_column_num, int record, String condition_t){
        table_id =  table_id_;
        key_column = key_column_num;
        join_column = join_column_num;
        record_num = record;
        condition = condition_t;
    }

    public void construct(){
        //Step 1 read the dataset from the tables
        //Step 1.1 prepare the parameters
        id = new String[record_num + 1];
        keyword = new String[record_num + 1][key_column];
        join_attr = new String[record_num + 1][join_column];
        Map<String, ArrayList<Integer>> reverse_id = new LinkedHashMap<>(); //the pairs of (attribute-value pair, record ids)
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
                        String kword = keyword[0][j] + record.get(j + 1);   //attribute-value pair
                        if (!reverse_id.containsKey(kword))
                            reverse_id.put(kword, new ArrayList<>());
                        reverse_id.get(kword).add(counter);
                    }
                }
                //note that join-attribute values pairs are also seen as the attribute-value pair
                for (int j = 0; j < join_column; j++) {
                    join_attr[counter][j] = record.get(key_column + j + 1);
                    if (counter != 0){
                        String kword = join_attr[0][j] + record.get(key_column + j + 1);
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
        //Step 2.1 compute the XSet entries
        long[] xy = new long[record_num * join_column];     //represents the xInd * yInd in the JXT paper
        byte[][] x = new byte[record_num * join_column][];  // xInd
        byte[][] y = new byte[record_num * join_column][];  // yInd
        for (int i = 1; i <= record_num; i++) {
            for (int j = 0; j < join_column; j++) {
                x[(i - 1) * join_column + j] = Hash.Get_SHA_256((K_x + id[i] + join_attr[0][j]).getBytes(StandardCharsets.UTF_8));
                y[(i - 1) * join_column + j] = Hash.Get_SHA_256((K_y + join_attr[i][j]).getBytes(StandardCharsets.UTF_8));
                xy[(i - 1) * join_column + j] = tool.bytesToLong(tool.Xor(x[(i - 1) * join_column + j], y[(i - 1) * join_column + j]));
            }
        }
        f = Bloom.construct(xy, 64);//XSet
        //Step 2.2 compute the TSet entries
        for(String kword : reverse_id.keySet()){//for each attribute-value pair
            BigInteger token = new BigInteger(Hash.Get_SHA_256((K_token + kword + table_id).getBytes(StandardCharsets.UTF_8)));
            ArrayList<Integer> reverse_tmp = reverse_id.get(kword);
            byte[] h1_0 = Hash.Get_SHA_256((K_h1 + kword + 0).getBytes(StandardCharsets.UTF_8));//Z_0 = F(w||0)
            byte[] h2_0 = Hash.Get_SHA_256((K_h2 + kword + 0).getBytes(StandardCharsets.UTF_8));//Z'_0 = F(w||0)
            ArrayList<tuple> t = new ArrayList<>();
            for (int i = 0; i < reverse_tmp.size(); i++) {//for each record
                int record_id = reverse_tmp.get(i);
                byte[] h1 = Hash.Get_SHA_256((K_h1 + kword + (i + 1)).getBytes(StandardCharsets.UTF_8));//Z_cnt = F(w||cnt)
                byte[] h2 = Hash.Get_SHA_256((K_h2 + kword + (i + 1)).getBytes(StandardCharsets.UTF_8));//Z'_cnt = F(w||cnt)
                tuple tset_each = new tuple();
                tset_each.h_x = new byte[join_column][];
                tset_each.h_y = new byte[join_column][];
                for (int j = 0; j < join_column; j++) {//for each join attribute
                    byte[] x_t = x[(record_id - 1) * join_column + j];
                    byte[] y_t = y[(record_id - 1) * join_column + j];
                    tset_each.h_x[j] = tool.Xor(h1, tool.Xor(h1_0, x_t));
                    tset_each.h_y[j] = tool.Xor(h2, tool.Xor(h2_0, y_t));
                }
                byte[] K_enc = Hash.Get_SHA_256((K_aes + kword).getBytes());
                byte[] ct = AESUtil.encrypt(K_enc, id[record_id].getBytes(StandardCharsets.UTF_8));
                tset_each.ct = ct;
                t.add(tset_each);
            }
            tset.put(token, t);
        }
    }

    public Map<BigInteger, ArrayList<tuple>> getTset() {
        return tset;
    }

    public Bloom getF() {
        return f;
    }

    public void Store(String text) {//store the TSet and XSet entries
        try {
            FileOutputStream file = new FileOutputStream("data/EDB/JXT_" + text + ".dat");
            //table1
            for (BigInteger token : tset.keySet()) {
                file.write(token.toByteArray());
                ArrayList<tuple> tuples = tset.get(token);
                for (tuple t : tuples){
                    file.write(t.ct);
                    for (int i = 0; i < t.h_x.length; i++) {
                        file.write(t.h_x[i]);
                        file.write(t.h_y[i]);
                    }
                }
            }
            byte[][] xset1 = f.getData();
            for (int i = 0; i < xset1.length; i++) {
                file.write(xset1[i]);
            }
            file.close();
        } catch (IOException e) {
            System.out.println("Error - " + e.toString());
        }
    }
}
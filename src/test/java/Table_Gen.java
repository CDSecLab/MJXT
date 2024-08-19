import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * This code aims to generate the table with different numbers of join attributes.
 *
 * @Author: 杜凯
 * @Date: 2023/11/06/15:29
 * @Description:
 */
public class Table_Gen {
    static String table_name = "table6";
    static String condition = "";
    static String not_meet = "f_";
    static int key_colnum = 9;
    static int join_column = 10 - key_colnum;
    static int record_num = (int)Math.pow(2, 16);//65536

    /**
     * write the records that all match. e.g., attribute-value pair w matching 1000 records which all satisfy the query
     * @param buffWriter write buffer
     * @param th the column number
     * @param type the number of records for each kind
     * @param counter the counter
     * @return the counter
     * @throws IOException
     */
    public static int write_all_matched(BufferedWriter buffWriter, int th, int type, int counter) throws IOException {
        int share_num = 2;//the number of the identical record
        for (int i = 0; i < type; i++) {
            for (int j = 0; j < share_num; j++) {
                String csv = table_name + "_id_" + counter;
                for (int k = 0; k < key_colnum; k++) {
                    csv += "," + table_name +"_keyword_" + th + "_" + k;
                }
                for (int k = 0; k < join_column; k++) {
                    csv += "," + "join_" + th + "_"+ i + "_" + k;
                }
                counter++;
                buffWriter.write(csv);
                buffWriter.newLine();
                buffWriter.flush();
            }
        }
        return counter;
    }

    /**
     * write the records that all match. e.g., w matching 1000 records where 100 records satisfy the query.
     * @param buffWriter
     * @param th the column number
     * @param all_type total number of the record for the w, i.e., 1000
     * @param part_type partly number, 100,200,...,1000
     * @param counter
     * @return
     * @throws IOException
     */
    public static int write_partly_matched(BufferedWriter buffWriter, int th, int all_type, int part_type, int counter) throws IOException {
        int share_num = 2;
        int cnt = 0;
        for (int i = 0; i < all_type; i++) {
            for (int k = 0; k < share_num; k++) {
                String csv = table_name + "_id_" + counter;
                if (cnt < part_type){
                    for (int j = 0; j < key_colnum; j++) {
                        csv += "," + table_name +"_keyword_" + th + "_" + j;
                    }
                    for (int j = 0; j < join_column; j++) {
                        csv += "," + "join_" + th + "_" + i + "_" + j;
                    }
                }else {
                    for (int j = 0; j < key_colnum; j++) {
                        csv += "," + table_name +"_keyword_" + th + "_" + j;
                    }
                    for (int j = 0; j < join_column; j++) {
                        csv += "," + "join_" + th + "_" + not_meet + i + "_" + j;
                    }
                }
                counter++;
                buffWriter.write(csv);
                buffWriter.newLine();
                buffWriter.flush();
            }
            cnt++;
        }
        return counter;
    }

    public static void main(String[] args) {
        BufferedWriter buffWriter = null;
        try {
            //the output file location
            File csvFile = new File("data/" + table_name + "/" + table_name + "_k" + key_colnum
                    + "_j" + join_column + "_" + record_num + condition +".csv");
            FileWriter writer = new FileWriter(csvFile, false);
            buffWriter = new BufferedWriter(writer, 1024);
            //write the column names
            String title = "id";
            for (int i = 0; i < key_colnum; i++) {
                title += ",keyword" + i;
            }
            for (int i = 0; i < join_column; i++) {
                title += ",join-attr" + i;
            }
            buffWriter.write(title + "\n");

            int counter = 0;

            for (int i = 0; i < 10; i++) {
                counter = write_all_matched(buffWriter, i, (i + 1) * 500, counter);
            }
            for (int i = 0; i < 10; i++) {
                counter =  write_partly_matched(buffWriter, i + 10, 500, (i + 1) * 100, counter);
            }

            System.out.println(counter);
            for (int i = counter; i < record_num; i++) {
                String csv = table_name + "_id_" + counter;
                for (int j = 0; j < key_colnum; j++) {
                    csv += "," + table_name +"_keyword_" + counter + "_" + j;
                }
                for (int j = 0; j < join_column; j++) {
                    csv += "," + "join_" + counter + "_" + j;
                }
                counter++;
                buffWriter.write(csv);
                buffWriter.newLine();
                buffWriter.flush();
            }
        } catch (Exception e) {
            System.out.println("Exception of writing into csv.");
            e.printStackTrace();
        } finally {
            try {
                if (buffWriter != null) {
                    buffWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

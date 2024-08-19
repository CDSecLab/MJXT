import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 *
 *
 * @Author: 杜凯
 * @Date: 2023/11/06/15:29
 * @Description:
 */
public class Table_Gen_Lmax {
    static String table_name = "table2";
    static int key_colnum = 9;
    static int join_column = 1;
    static int record_num = (int) Math.pow(2, 16);//65536
    static int Lmax = 1000;
    static String condition = "_Lmax" + Lmax; //same_num or same_type

    public static int write_Lmax(BufferedWriter buffWriter, int num, int L_max, int counter) throws IOException {
        for (int i = 0; i < num; i++) {
            String csv = table_name + "_id_" + counter;
            for (int j = 0; j < key_colnum; j++) {
                csv += "," + table_name +"_keyword_0_" + j;
            }

            for (int j = 0; j < join_column; j++) {
                int mod = counter / L_max;
                csv += "," + "join_0_" + mod + "_" + j;
            }
            counter++;
            buffWriter.write(csv);
            buffWriter.newLine();
            buffWriter.flush();
        }
        return counter;
    }

    public static void main(String[] args) {
        BufferedWriter buffWriter = null;
        try {
            File csvFile = new File("data/" + table_name + "/" + table_name+ "_k" + key_colnum
                    + "_j" + join_column + "_" + record_num + condition +".csv");
            FileWriter writer = new FileWriter(csvFile, false);
            buffWriter = new BufferedWriter(writer, 1024);
            String title = "id";
            for (int i = 0; i < key_colnum; i++) {
                title += ",keyword" + i;
            }
            for (int i = 0; i < join_column; i++) {
                title += ",join-attr" + i;
            }
            buffWriter.write(title + "\n");

            int counter = 0;
            //int[] L_max = new int[]{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000};
            counter = write_Lmax(buffWriter, 1000, Lmax, counter);
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

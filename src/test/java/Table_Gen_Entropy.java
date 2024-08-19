import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * This code aims to generate the tables with different entropy.
 *
 * @Author: 杜凯
 * @Date: 2024/03/18/11:32
 * @Description:
 */
public class Table_Gen_Entropy {

    public static void main(String[] args) {
        BufferedWriter buffWriter = null;
        //entropy->repeat : 16->2^8, 14->2^6, 12->2^4
        int entropy = 14;
        int repeat = (int) Math.pow(2, 6);
        try {
            File csvFile = new File("data/table1/table1_k9_j1_65536_" + entropy + ".csv");
            FileWriter writer = new FileWriter(csvFile, false);
            buffWriter = new BufferedWriter(writer, 1024);
            String title = "id,keyword0,keyword1,keyword2,keyword3,keyword4,keyword5,keyword6," +
                    "keyword7,keyword8,join-attr0\n";
            buffWriter.write(title);
            buffWriter.flush();
            int counter = 0;
            for (int i = 0; i < Math.pow(2, 8); i++) {
                for (int j = 0; j < Math.pow(2, 8) / repeat; j++) {
                    for (int k = 0; k < repeat; k++) {
                        String csv = "table1_id_" + counter;
                        csv += "," + "table1_keyword_0_" + i;
                        csv += "," + "table1_keyword_1_" + i;
                        csv += "," + "table1_keyword_2_" + i;
                        csv += "," + "table1_keyword_3_" + i;
                        csv += "," + "table1_keyword_4_" + i;
                        csv += "," + "table1_keyword_5_" + i;
                        csv += "," + "table1_keyword_6_" + i;
                        csv += "," + "table1_keyword_7_" + i;
                        csv += "," + "table1_keyword_8_" + i;
                        csv += "," + "join_" + i + "_" + k;
                        counter++;
                        buffWriter.write(csv);
                        buffWriter.newLine();
                        buffWriter.flush();
                    }
                }
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

import client.Setup_JXT;
import client.Setup_JXTp;
import client.Setup_JXTpp;

/**
 * This java file aims to test the setup time cost of JXT,JXT+,JXT++ table-wise on the dataset with different join attributes.
 * This code can generate Figure 4 in the paper.
 * @Author: 杜凯
 * @Date: 2023/11/18/15:47
 * @Description:
 */
public class test_Setup {
    public static void main(String[] args) {

        int join_column = 1;
        int key_colnum = 10 - join_column;
        int record_num = (int)Math.pow(2, 16);//65536

        long time_JXT = 0;
        long time_JXTp = 0;
        long time_JXTpp = 0;
        String condition = "";//used to choose different tables from dataset, e.g., "_14".
        int circle_num = 10;//the run times, for one circle it takes about 30 s.
        for (int j = 0; j < circle_num; j++) {
            //System.out.println("------------- JXT ---------------");
            long setup_start_JXT = System.nanoTime();
            Setup_JXT table_JXT = new Setup_JXT(1, key_colnum, join_column, record_num, condition);
            table_JXT.construct();
            long setup_end_JXT = System.nanoTime();
            time_JXT += setup_end_JXT - setup_start_JXT;
            //System.out.println("JXT setup time : " + (setup_end_JXT - setup_start_JXT)/Math.pow(10, 6) + " ms");

            //System.out.println("------------- JXT+ ---------------");
            long setup_start_JXTp = System.nanoTime();
            Setup_JXTp table_JXTp = new Setup_JXTp(1, key_colnum, join_column, record_num, condition);
            table_JXTp.construct();
            long setup_end_JXTp = System.nanoTime();
            time_JXTp += setup_end_JXTp - setup_start_JXTp;
            //System.out.println("JXT+ setup time : " + (setup_end_JXTp - setup_start_JXTp)/Math.pow(10, 6) + " ms");

            //System.out.println("------------- JXT++ ---------------");
            long setup_start_JXTpp = System.nanoTime();
            Setup_JXTpp[] table_JXTpp = new Setup_JXTpp[1];
            table_JXTpp[0] = new Setup_JXTpp(1, key_colnum, join_column, record_num, "");
            table_JXTpp[0].construct();
            long setup_end_JXTpp = System.nanoTime();
            time_JXTpp += setup_end_JXTpp - setup_start_JXTpp;
            //System.out.println("JXT++ setup time : " + (setup_end_JXTpp - setup_start_JXTpp)/Math.pow(10, 6) + " ms");
        }
        System.out.println("JXT setup average time : " + time_JXT / Math.pow(10, 9 + circle_num / 10) + " s");
        System.out.println("JXT+ setup average time : " + time_JXTp / Math.pow(10, 9+ circle_num / 10) + " s");
        System.out.println("JXT++ setup average time : " + time_JXTpp / Math.pow(10, 9+ circle_num / 10) + " s");
    }

}

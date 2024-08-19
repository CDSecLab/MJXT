import client.Setup_JXT;
import client.Setup_JXTp;
import client.Setup_JXTpp;

/**
 * This java file aims to test the storage overhead of JXT,JXT+,JXT++ table-wise on the dataset with different join attributes.
 * This code can generate Figure 1 in the paper. Note that the EDB will output in location "data/EDB/...".
 *  This code can generate Table 3 in the paper by select data/table1/table1_k1_j1_65536_16,_14,_12
 * @Author: 杜凯
 * @Date: 2024/03/18/12:00
 * @Description:
 */
public class test_storage {
    public static void main(String[] args) {
        //the number join attributes in the table.
        int join_column = 1;//the values{1, 2, 3, 4, 5} for our dataset.
        //the number of attributes which aren't the join attribute in the table.
        int key_colnum = 10 - join_column;//the values{9, 8, 7, 6, 5} for our dataset
        int record_num = (int)Math.pow(2, 16);//65536
        String condition = "";// "_16", "_14", "_12"
        String id = "1";

        System.out.println("-------------- JXT begin to setup-------------");
        Setup_JXT table_JXT = new Setup_JXT(1, key_colnum, join_column, record_num, condition);
        table_JXT.construct();
        table_JXT.Store(id);
        System.out.println("------------- JXT setup complete -------------");
        System.out.println("------------- JXT+ begin to setup-------------");
        Setup_JXTp table_JXTp = new Setup_JXTp(1, key_colnum, join_column, record_num, condition);
        table_JXTp.construct();
        table_JXTp.Store(id);
        System.out.println("------------- JXT+ setup complete ------------");
        System.out.println("------------- JXT++ begin to setup------------");
        Setup_JXTpp[] table_JXTpp = new Setup_JXTpp[1];
        table_JXTpp[0] = new Setup_JXTpp(1, key_colnum, join_column, record_num, condition);
        table_JXTpp[0].construct();
        table_JXTpp[0].Store(id);
        System.out.println("------------ JXT++ setup complete ------------");
        System.out.println("The output of EDB is in data/EDB");
    }
}

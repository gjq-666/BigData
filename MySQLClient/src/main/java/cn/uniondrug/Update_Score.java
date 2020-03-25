package cn.uniondrug;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author ：gjq
 * @date ：Created in 2020/3/11 13:59
 * @Function :Update Score
 */
public class Update_Score {
    public static void main(String[] args) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection cnn = DriverManager.getConnection("jdbc:mysql://rm-bp1nq6ibu5s4noqbo310.mysql.rds.aliyuncs.com:3306/cn_uniondrug_module_score", "uniondrug", "juyin@2017");

           String sql = "UPDATE cn_uniondrug_module_score.score\n" +
                   "SET partnerNum = 0,\n" +
                   "    storeNum   = 0\n" +
                   "WHERE id > 0;";
           String sql1 = "UPDATE cn_uniondrug_module_score.score a,\n" +
                   "    (SELECT *, SUM(storeCount) storeCounts, COUNT(*) partnerCount\n" +
                   "     FROM cn_uniondrug_module_merchant.score_partner\n" +
                   "     WHERE scoreDay = (SELECT max(scoreDay)\n" +
                   "                       FROM cn_uniondrug_module_merchant.score_partner)\n" +
                   "     GROUP BY uniqueCode) c\n" +
                   "SET a.partnerNum = c.partnerCount,\n" +
                   "    a.storeNum   = c.storeCounts\n" +
                   "WHERE a.uniqueCode = c.uniqueCode;";
            PreparedStatement preparedStatement = cnn.prepareStatement(sql);
            PreparedStatement preparedStatement1 = cnn.prepareStatement(sql1);
            preparedStatement.execute();
            preparedStatement1.execute();
            System.out.println("update success");
            cnn.close();
        } catch (SQLException var4) {
            var4.printStackTrace();
        } catch (ClassNotFoundException var5) {
            var5.printStackTrace();
        }

    }
}

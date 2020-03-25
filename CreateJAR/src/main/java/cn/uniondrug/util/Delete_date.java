package cn.uniondrug.util;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
public class Delete_date {

    public static void main(String[] args) {

        try {
            String statis_date=args[0];
            System.out.println(statis_date);
            Class.forName("com.mysql.jdbc.Driver");
            Connection cnn = DriverManager.getConnection("jdbc:mysql://582959f06c18d.sh.cdb.myqcloud.com:3712/test", "develop", "develop123");
            StringBuilder sql = new StringBuilder();
            sql.append("delete from  t where id=");
            sql.append(statis_date);
            PreparedStatement preparedStatement = cnn.prepareStatement(String.valueOf(sql));
            preparedStatement.execute();
            System.out.println("truncate success");
            cnn.close();
        } catch (SQLException var4) {
            var4.printStackTrace();
        } catch (ClassNotFoundException var5) {
            var5.printStackTrace();
        }
        

    }
}

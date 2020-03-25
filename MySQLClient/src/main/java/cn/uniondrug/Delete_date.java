package cn.uniondrug;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Delete_date {
    public static void main(String[] args) {

        try {
            //日期格式转化  2020-03-08
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date date = sdf.parse(args[0]);    //将字符串转化为日期类型()
            String statis_date = sdf.format(date);  //再将日期类型格式化成yyyy-MM-dd
            //System.out.println(statis_date);

            //加载驱动
            Class.forName("com.mysql.jdbc.Driver");
            Connection cnn = DriverManager.getConnection("jdbc:mysql://rm-bp1nq6ibu5s4noqbo310.mysql.rds.aliyuncs.com:3306/cn_uniondrug_module_merchant", "uniondrug", "juyin@2017");
            StringBuilder sql = new StringBuilder();
            //String sql = "delete from  score_partner where scoreDay="+statis_date;
            sql.append("delete from  score_partner where scoreDay='");
            sql.append(statis_date);
            sql.append("'");
            System.out.printf(String.valueOf(sql));
            PreparedStatement preparedStatement = cnn.prepareStatement(String.valueOf(sql));
            preparedStatement.execute();
            System.out.println("truncate success");
            cnn.close();
        } catch (SQLException var4) {
            var4.printStackTrace();
        } catch (ClassNotFoundException var5) {
            var5.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }


    }

}

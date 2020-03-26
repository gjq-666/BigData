package cn.uniondrug.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Truncate table table_name;  清空表数据
 * */

public class Truncate_Data {

    private static final String ORDER_CONFIG_PATH = "/mysql.properties";
    private static Properties prop = new Properties();

    public static void main(String[] args){


        try {
                prop.load(Truncate_Data.class.getResourceAsStream(ORDER_CONFIG_PATH));

            //jdbc连接及操作
            Class.forName(prop.getProperty("jdbcDriver"));
            Connection conn = DriverManager.getConnection(prop.getProperty("url"),prop.getProperty("user"),prop.getProperty("password"));
            Statement stat = conn.createStatement();

            //store_isbill_detail
            String sql = "Truncate table t";
            stat.executeUpdate(sql);
            System.out.println("清空数据成功");
            conn.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


}

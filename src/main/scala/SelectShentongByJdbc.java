import org.apache.log4j.Logger;

import java.sql.*;

public class SelectShentongByJdbc {


    public static void main(String[] args) throws SQLException, InterruptedException {
        Logger logger = Logger.getLogger(SelectShentongByJdbc.class);
        String url = "jdbc:oscar://192.168.0.87:2003/osrdb?useUnicode=true&amp;characterEncoding=utf-8;useOldAliasMetadataBehavior=true";
        try {
            Class.forName("com.oscar.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection connection = DriverManager.getConnection(url, "sysdba", "szoscar55");
        Statement statement = connection.createStatement();

        String sql = "select count(1) as count_number from TEST.shentong_person_01";
        int a = 0;
        while (true) {
            a += 1;
            long startTime = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            resultSet.next();
            long endTime = System.currentTimeMillis();
            logger.error("第" + a + "次查询出来条数：" + resultSet.getLong(1) + ",查询总耗时(单位:ms)：" + (endTime - startTime));
            Thread.sleep(1000);//1秒
        }
    }
}

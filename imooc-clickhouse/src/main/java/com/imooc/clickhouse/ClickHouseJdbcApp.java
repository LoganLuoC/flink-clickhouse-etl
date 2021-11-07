package com.imooc.clickhouse;

import java.sql.*;

public class ClickHouseJdbcApp {
    public static void main(String[] args) throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String url = "jdbc:clickhouse://localhost:8123/dd";
        Connection connection = DriverManager.getConnection(url);
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from test3_emp");
        while (rs.next()) {
            int id = rs.getInt("EMPNO");
            String name = rs.getString("ENAME");
            System.out.println(id + "====>" + name);
        }

        rs.close();
        stmt.close();
        connection.close();

    }
}

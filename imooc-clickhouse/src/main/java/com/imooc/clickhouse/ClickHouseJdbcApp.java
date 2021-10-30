package com.imooc.clickhouse;

import java.sql.*;

public class ClickHouseJdbcApp {
    public static void main(String[] args) throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String url = "jdbc:clickhouse://localhost:8123/ck";
        Connection connection = DriverManager.getConnection(url);
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from user");
        while (rs.next()) {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            rs.getString(id + "====>" + name);
        }

        rs.close();
        stmt.close();
        connection.close();

    }
}

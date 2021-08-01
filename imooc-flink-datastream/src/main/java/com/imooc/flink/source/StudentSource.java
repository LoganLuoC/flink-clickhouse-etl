package com.imooc.flink.source;

import com.imooc.flink.utils.MySQLUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class StudentSource extends RichSourceFunction<Student> {
    Connection connection;
    PreparedStatement psmt;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = MySQLUtils.getConnection();
         psmt = connection.prepareStatement("select * from student");
    }

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet res = psmt.executeQuery();
        while (res.next()) {
            int id = res.getInt("id");
            String name = res.getString("name");
            int age = res.getInt("age");

            ctx.collect(new Student(id, name, age));
        }
    }

    @Override
    public void close() throws Exception {
        MySQLUtils.close(connection, psmt);
    }

    @Override
    public void cancel() {

    }
}

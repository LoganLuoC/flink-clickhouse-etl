package com.imooc.flink.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

public class PKMapFunction extends RichMapFunction<String, Access> {
    /**
     * 初始化操作, 生命周期
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("open");
    }

    /**
     * 清理
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return super.getRuntimeContext();
    }

    /**
     * 业务逻辑
     * @param s
     * @return
     * @throws Exception
     */
    @Override
    public Access map(String s) throws Exception {
        System.out.println("=====map======");

        String[] splits = s.split(",");
        long time = Long.parseLong(splits[0].trim());
        String domain = splits[1].trim();
        Double traffic = Double.parseDouble(splits[2].trim());
        return new Access(time, domain, traffic);
    }

}

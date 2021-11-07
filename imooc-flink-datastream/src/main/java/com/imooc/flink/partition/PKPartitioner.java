package com.imooc.flink.partition;

import org.apache.flink.api.common.functions.Partitioner;

public class PKPartitioner implements Partitioner {
    @Override
    public int partition(Object key, int numPartitions) {
        System.out.println("numPartitions ..." + numPartitions);

        if ("baidu.com".equals(key)) {
            return 0;
        } else  if ("a.com".equals(key)) {
            return 1;
        } else {
            return 2;
        }
    }
}

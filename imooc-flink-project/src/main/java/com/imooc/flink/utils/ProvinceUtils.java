package com.imooc.flink.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.imooc.flink.domian.Access;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class ProvinceUtils extends RichMapFunction<Access, Access> {

    CloseableHttpClient httpClient = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        httpClient = HttpClients.createDefault();
    }

    @Override
    public void close() throws Exception {
        if (httpClient != null) httpClient.close();
    }

    @Override
    public Access map(Access value) throws Exception {
        String  ip = value.ip;
        String url = "https://restapi.amap.com/v5/ip?ip=" + ip + "&type=4&key=" + StringUtils.GAODE_KEY;

        String province = "";
        String city = "";

        CloseableHttpResponse response = null;

        try {
            HttpGet httpGet = new HttpGet(url);
            response = httpClient.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();

            if (statusCode == 200) {
                HttpEntity entity = response.getEntity();
                String result = EntityUtils.toString(entity, "UTF-8");

                JSONObject jsonObject = JSON.parseObject(result);
                province = jsonObject.getString("province");
                city = jsonObject.getString("city");

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != response) {
                try {
                    response.close();
                    value.province = province;
                    value.city = city;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return value;
    }
}

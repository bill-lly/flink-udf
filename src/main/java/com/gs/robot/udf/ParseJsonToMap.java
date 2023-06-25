package com.gs.robot.udf;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;


public class ParseJsonToMap extends ScalarFunction {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(ParseJsonToMap.class);
    @DataTypeHint("MAP<STRING, STRING>")
    public HashMap<String, String> eval(String s) {
        HashMap<String, String> map = new HashMap<>();
        try {
            HashMap<String, Object> readValue = objectMapper.readValue(s, new TypeReference<HashMap<String, Object>>() {});
            Iterator<String> iterator = readValue.keySet().iterator();
            while (iterator.hasNext()){
                String key = iterator.next();
                Object value = readValue.get(key);
                map.put(key,objectMapper.writeValueAsString(value));
            }
            return map;
        } catch (JsonProcessingException e) {
            logger.error(e+":脏数据:传入数据格式非json格式：{\"key\":\"value\",\"key2\":\"value1\"},无法解析;");
            return null;
        }
    }
}
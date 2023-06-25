package com.gs.robot.udtf;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;

@FunctionHint(output = @DataTypeHint("ARRAY<STRING>"))
public class ExplodeJSONArraysByIndexAndNextN extends TableFunction<String []> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(ExplodeJSONArraysByIndexAndNextN.class);

    /**
     *
     * @param n 向下取n行
     * @param args 传入多个字段，jsonArray，按照顺序炸开
     */
    public void eval(int n,String... args) {
        if (args != null) {
            // 结果数组，长度等于返回的字段数,+1字段用于记录index
            String[] result = new String[args.length*2+1];
            // 把每一个传入的字段解析为jsonArray然后存放到集合中
            ArrayList<ArrayList<Object>> jsonArrayList = new ArrayList<>();
            // 记录每个元素的长度
            int[] lengths = new int[args.length];
            for (int i = 0; i < args.length; i++) {
                ArrayList<Object> list;
                try {
                    list = objectMapper.readValue(args[i], new TypeReference<ArrayList<Object>>() {});
                } catch (JsonProcessingException e) {
                    logger.warn("Input Data:"+args[i]);
                    logger.warn(e.getMessage() + ";Data Format Exception : The input data format is not a json array " +
                            "structure and" +
                            " cannot be parsed");
                    list = new ArrayList<>();
                }catch (IllegalArgumentException ie){
                    logger.warn(ie.getMessage() + ";Data Format Exception : The input data format is null and cannot " +
                            "be" +
                            " parsed");
                    list = new ArrayList<>();
                }
                lengths[i] = list != null ? list.size() : 0;
                jsonArrayList.add(list);
            }
            // 求出所有输入jsonArray的最大长度
            int[] sortLengths = Arrays.stream(lengths).sorted().toArray();
            int maxLengthJsonArray = sortLengths[sortLengths.length - 1];
            if (maxLengthJsonArray>0) {
                // TODO 遍历返回
                for (int j = 0; j < maxLengthJsonArray; j++) {
                    for (int i = 0; i < jsonArrayList.size(); i++) {
                        String json;
                        String nextNJson;
                        // 遍历
                        // 取第i个jsaonArray的第j个json
                        try {
                            json = objectMapper.writeValueAsString(jsonArrayList.get(i).get(j));
                        } catch (IndexOutOfBoundsException je) {
                            json = "{}";
                        } catch (JsonProcessingException e) {
                            logger.error("object转string失败，转换数据为：" + jsonArrayList.get(i).get(j).toString());
                            json = "{}";
                        }
                        // 取第i个jsaonArray的第j+n个json
                        try {
                            nextNJson = objectMapper.writeValueAsString(jsonArrayList.get(i).get(j+n));
                        } catch (IndexOutOfBoundsException je) {
                            nextNJson = "{}";
                        } catch (JsonProcessingException e) {
                            logger.error("object转string失败，转换数据为：" + jsonArrayList.get(i).get(j).toString());
                            nextNJson = "{}";
                        }
                        result[i] = json;
                        result[i+args.length] = nextNJson;
                    }
                    // 上游回传数据的index有可能是错误的，单独增加一个字段记录实际index
                    result[result.length-1]= String.valueOf(j);
                    collect(result);
                }
            }else {
                // 入参都是null,无法炸开,index记录0并返回
                result[result.length-1]= String.valueOf(0);
                collect(result);
            }
        }else {
            throw new IllegalArgumentException("ERROR:参数为空,请至少传入一个参数");
        }
    }
}

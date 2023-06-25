package com.gs.robot.udf;

import com.gs.robot.util.JsonUtil;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import sun.misc.BASE64Decoder;

import java.nio.charset.StandardCharsets;

public class RobotState10Parse extends ScalarFunction  {

  private static final Logger logger = LoggerFactory.getLogger(RobotState10Parse.class);
  private static final BASE64Decoder decoder = new BASE64Decoder();
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @DataTypeHint("ROW<createdAt string, deviceLevels string, deviceStatus string, floorBinding string, healthStatus string, initializeStatus string, mapList string, mistType string, modelType string, newCleaningModes string, osStatus string, position string, positionList string, privacyMode string, productId string, robotStatus string, supportCleaningModes string, supportFeatureList string, taskQueueList string, trajectory string, workState string, workStatus string>")
  public Row eval(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      byte[] by = decoder.decodeBuffer(s);
      JsonNode jsonNode = objectMapper.readTree(Snappy.uncompressString(by, StandardCharsets.UTF_8));
      return Row.of(JsonUtil.getText(jsonNode.get("created_at")),
              JsonUtil.getJsonString(jsonNode.get("deviceLevels")),
              JsonUtil.getJsonString(jsonNode.get("device_status")),
              JsonUtil.getJsonString(jsonNode.get("floorBinding")),
              JsonUtil.getJsonString(jsonNode.get("health_status")),
              JsonUtil.getJsonString(jsonNode.get("initialize_status")),
              JsonUtil.getJsonString(jsonNode.get("mapList")),
              JsonUtil.getText(jsonNode.get("mistType")),
              JsonUtil.getText(jsonNode.get("modelType")),
              JsonUtil.getJsonString(jsonNode.get("newCleaningModes")),
              JsonUtil.getJsonString(jsonNode.get("os_status")),
              JsonUtil.getJsonString(jsonNode.get("position")),
              JsonUtil.getJsonString(jsonNode.get("positionList")),
              JsonUtil.getText(jsonNode.get("privacyMode")),
              JsonUtil.getText(jsonNode.get("product_id")),
              JsonUtil.getJsonString(jsonNode.get("robotStatus")),
              JsonUtil.getJsonString(jsonNode.get("supportCleaningModes")),
              JsonUtil.getJsonString(jsonNode.get("supportFeatureList")),
              JsonUtil.getJsonString(jsonNode.get("taskQueueList")),
              JsonUtil.getJsonString(jsonNode.get("trajectory")),
              JsonUtil.getJsonString(jsonNode.get("workState")),
              JsonUtil.getJsonString(jsonNode.get("work_status")));
    } catch (Exception e) {
      logger.error(String.format("failed to uncompress the data in snappy, data=%s", s), e);
      return null;
    }
  }
}

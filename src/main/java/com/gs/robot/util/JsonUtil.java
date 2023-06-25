package com.gs.robot.util;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class JsonUtil {

  public static String getJsonString(JsonNode jsonNode) {
    if (jsonNode == null) {
      return null;
    }
    return jsonNode.toString();
  }

  public static String getText(JsonNode jsonNode) {
    if (jsonNode == null) {
      return null;
    }
    return jsonNode.asText();
  }
}

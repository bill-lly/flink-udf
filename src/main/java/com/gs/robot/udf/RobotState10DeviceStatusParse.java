package com.gs.robot.udf;

import com.gs.robot.util.JsonUtil;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RobotState10DeviceStatusParse extends ScalarFunction {

  private static final Logger logger = LoggerFactory.getLogger(RobotState10DeviceStatusParse.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @DataTypeHint("ROW<fourGSignalStrength String, locationNeedConfirmElevator String, soc String, battery String, batteryCellNumber String, batteryCurrent String, batteryHalfVoltage String, batteryVoltage String, charge String, chargeCurrent String, chargeNum String, charger String, chargerCurrent String, chargerPileStatus String, chargerStatus String, chargerVoltage String, detailedChargerCurrentAdc String, detailedChargerVoltageAdc String, detailedCountsLeft String, detailedCountsRight String, manualChargerConnection String, manualCharging String, mileage String, mileageLeft String, mileageRight String, leftMotorCurrent String, rightMotorCurrent String, leftMotorTemperature String, rightMotorTemperature String, leftSideBrushUsage String, rightSideBrushUsage String, rollerSqueegeeUsage String, rollerSqueegeeUsageAlert String, rollingBrushMotor String, rollingBrushPressureLevel String, rollingBrushPressureValue String, rollingBrushUsage String, rollingBrushUsageAlert String>")
  public Row eval(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      JsonNode deviceStatus = objectMapper.readTree(s);
      if (!deviceStatus.has("data")) {
        return null;
      }
      JsonNode jsonNode = deviceStatus.get("data");
      return Row.of(JsonUtil.getText(jsonNode.get("4GSignalStrength")),
            JsonUtil.getText(jsonNode.get("locationNeedConfirmElevator")),
            JsonUtil.getText(jsonNode.get("soc")),
            JsonUtil.getText(jsonNode.get("battery")),
            JsonUtil.getText(jsonNode.get("batteryCellNumber")),
            JsonUtil.getText(jsonNode.get("batteryCurrent")),
            JsonUtil.getText(jsonNode.get("batteryHalfVoltage")),
            JsonUtil.getText(jsonNode.get("batteryVoltage")),
            JsonUtil.getText(jsonNode.get("charge")),
            JsonUtil.getText(jsonNode.get("chargeCurrent")),
            JsonUtil.getText(jsonNode.get("chargeNum")),
            JsonUtil.getText(jsonNode.get("charger")),
            JsonUtil.getText(jsonNode.get("chargerCurrent")),
            JsonUtil.getText(jsonNode.get("chargerPileStatus")),
            JsonUtil.getText(jsonNode.get("chargerStatus")),
            JsonUtil.getText(jsonNode.get("chargerVoltage")),
            JsonUtil.getText(jsonNode.get("detailedChargerCurrentAdc")),
            JsonUtil.getText(jsonNode.get("detailedChargerVoltageAdc")),
            JsonUtil.getText(jsonNode.get("detailedCountsLeft")),
            JsonUtil.getText(jsonNode.get("detailedCountsRight")),
            JsonUtil.getText(jsonNode.get("manualChargerConnection")),
            JsonUtil.getText(jsonNode.get("manualCharging")),
            JsonUtil.getText(jsonNode.get("mileage")),
            JsonUtil.getText(jsonNode.get("mileageLeft")),
            JsonUtil.getText(jsonNode.get("mileageRight")),
            JsonUtil.getText(jsonNode.get("leftMotorCurrent")),
            JsonUtil.getText(jsonNode.get("rightMotorCurrent")),
            JsonUtil.getText(jsonNode.get("leftMotorTemperature")),
            JsonUtil.getText(jsonNode.get("rightMotorTemperature")),
            JsonUtil.getText(jsonNode.get("leftSideBrushUsage")),
            JsonUtil.getText(jsonNode.get("rightSideBrushUsage")),
            JsonUtil.getText(jsonNode.get("rollerSqueegeeUsage")),
            JsonUtil.getText(jsonNode.get("rollerSqueegeeUsageAlert")),
            JsonUtil.getText(jsonNode.get("rollingBrushMotor")),
            JsonUtil.getText(jsonNode.get("rollingBrushPressureLevel")),
            JsonUtil.getText(jsonNode.get("rollingBrushPressureValue")),
            JsonUtil.getText(jsonNode.get("rollingBrushUsage")),
            JsonUtil.getText(jsonNode.get("rollingBrushUsageAlert")));
    } catch (Exception e) {
      logger.error(String.format("failed to uncompress the data in snappy, data=%s", s), e);
      return null;
    }
  }
}

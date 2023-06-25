package com.gs.robot.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RobotState20TaskPauseReason extends ScalarFunction{
    private static final Logger logger = LoggerFactory.getLogger(RobotState20TaskPauseReason.class);
    public String eval(String s) {
        if (s == null || s.isEmpty()) {
            return null;
        }
        int taskPauseReasonValue = Integer.parseInt(s);
        StringBuilder builder = new StringBuilder(10240);
        if((taskPauseReasonValue & 1) != 0) {
          builder.append("急停").append(",");
        }
        if((taskPauseReasonValue & 2) != 0) {
            builder.append("手动模式").append(",");
        }
        if((taskPauseReasonValue & 4) != 0) {
            builder.append("脚踏").append(",");
        }
        if((taskPauseReasonValue & 8) != 0) {
            builder.append("手动充电").append(",");
        }
        if((taskPauseReasonValue & 16) != 0) {
            builder.append("手动作业").append(",");
        }
        if((taskPauseReasonValue & 32) != 0) {
            builder.append("手动暂停").append(",");
        }
        if((taskPauseReasonValue & 64) != 0) {
            builder.append("静音模式/勿扰模式").append(",");
        }
        if((taskPauseReasonValue & 256) != 0) {
            builder.append("OTA 升级").append(",");
        }
        if((taskPauseReasonValue & 512) != 0) {
            builder.append("远程唤醒模式").append(",");
        }
        if((taskPauseReasonValue & 1024) != 0) {
            builder.append("远程控制").append(",");
        }
        if((taskPauseReasonValue & 65536) != 0) {
            builder.append("告警").append(",");
        }
        if((taskPauseReasonValue & 131072) != 0) {
            builder.append("告警").append(",");
        }
        if((taskPauseReasonValue & 262144) != 0) {
            builder.append("告警").append(",");
        }
        if((taskPauseReasonValue & 524288) != 0) {
            builder.append("告警").append(",");
        }
        if((taskPauseReasonValue & 1048576) != 0) {
            builder.append("告警").append(",");
        }
        if((taskPauseReasonValue & 2097152) != 0) {
            builder.append("告警").append(",");
        }
        if((taskPauseReasonValue & 16777216) != 0) {
            builder.append("调度").append(",");
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }
//    public static void main(String[] args) {
//        RobotState20TaskPauseReason fun = new RobotState20TaskPauseReason();
//        String s = "16777217";
//        String result = fun.eval(s);
//        System.out.println(result);
//    }
}

package com.gs.robot.udf;

import com.gs.robot.common.BASE64DecoderWrapper;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.nio.charset.StandardCharsets;

public class SnappyUncompress extends ScalarFunction {

  private static final Logger logger = LoggerFactory.getLogger(SnappyUncompress.class);
  private final BASE64DecoderWrapper decoder = new BASE64DecoderWrapper();

  public String eval(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      return Snappy.uncompressString(decoder.decodeBuffer(s), StandardCharsets.UTF_8);
    } catch (Exception e) {
      logger.error(String.format("failed to uncompress the data in snappy, data=%s", s), e);
      return null;
    }
  }
}

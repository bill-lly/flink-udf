package com.gs.robot.common;

import sun.misc.BASE64Decoder;

import java.io.IOException;

public class BASE64DecoderWrapper extends BASE64Decoder implements java.io.Serializable {

  public byte[] decodeBuffer(String s) throws IOException {
    return super.decodeBuffer(s);
  }
}

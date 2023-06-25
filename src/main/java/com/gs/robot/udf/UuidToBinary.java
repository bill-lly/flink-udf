package com.gs.robot.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.guava30.com.google.common.primitives.Bytes;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

public class UuidToBinary extends ScalarFunction{
    private static final Logger logger = LoggerFactory.getLogger(UuidToBinary.class);
    public byte[] eval(String str) {
        if (StringUtils.isBlank(str)) {
            logger.error("data format is not correct,data is null");
            return null;
        } else {
            UUID uuid = UUID.fromString(str);
            long msb = uuid.getMostSignificantBits();
            msb = msb << 48
                    | ((msb >> 16) & 0xFFFFL) << 32
                    | msb >>> 32;

            byte[] bytes = new byte[16];
            ByteBuffer.wrap(bytes)
                    .order(ByteOrder.BIG_ENDIAN)
                    .putLong(msb)
                    .putLong(uuid.getLeastSignificantBits());
            return bytes;
        }
    }

//test
//    public String toUuid(byte[] bytes) {
//        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
//        long msb = buf.getLong();
//        msb = msb << 32
//                | ((msb >> 32) & 0xFFFFL) << 16
//                | msb >>> 48;
//        long lsb = buf.getLong();
//        return new UUID(msb, lsb).toString();
//    }
//    public static void main(String[] args) {
//        UuidToBinary fun = new UuidToBinary();
//        byte[] bytes = fun.eval("b4cac394-e2ad-11ed-b0cc-8905abbab443");
//        String result = fun.toUuid(bytes);
//        System.out.println(result);
//    }
}

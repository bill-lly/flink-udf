package com.gs.robot.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.nio.ByteOrder;

public class BinaryToUuid extends ScalarFunction{
    private static final Logger logger = LoggerFactory.getLogger(BinaryToUuid.class);
    public String eval(byte[] bytes) {
        if (bytes == null) {
            logger.error("data format is not correct,data is null");
            return null;
        } else {
            ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
            long msb = buf.getLong();
            msb = msb << 32
                    | ((msb >> 32) & 0xFFFFL) << 16
                    | msb >>> 48;
            long lsb = buf.getLong();
            return new UUID(msb, lsb).toString();
        }
    }

//test
//    public byte[] toBinary(String str) {
//        UUID uuid = UUID.fromString(str);
//        long msb = uuid.getMostSignificantBits();
//        msb = msb << 48
//                | ((msb >> 16) & 0xFFFFL) << 32
//                | msb >>> 32;
//
//        byte[] bytes = new byte[16];
//        ByteBuffer.wrap(bytes)
//                .order(ByteOrder.BIG_ENDIAN)
//                .putLong(msb)
//                .putLong(uuid.getLeastSignificantBits());
//        return bytes;
//    }
//    public static void main(String[] args) {
//        BinaryToUuid fun = new BinaryToUuid();
//        byte[] bytes = fun.toBinary("b4cac394-e2ad-11ed-b0cc-8905abbab443");
//        String result = fun.eval(bytes);
//        System.out.println(result);
//    }
}

package com.gs.robot.udf;

import com.gs.robot.common.BASE64DecoderWrapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Decoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;


public class Decompress extends ScalarFunction {
    private static final Logger logger = LoggerFactory.getLogger(Decompress.class);
    private final BASE64DecoderWrapper base64Decoder = new BASE64DecoderWrapper();

    public String eval(String s) {
        if (StringUtils.isBlank(s)) {
            logger.error("data format is not correct,data is null");
            return null;
        } else {
            //TODO base64解码
            byte[] bytes;
            try {
                bytes = base64Decoder.decodeBuffer(s);
            } catch (IOException e) {
                logger.error("Data Format Is Not Correct,Decoder Failed");
                bytes = null;
            }
            if (bytes == null || bytes.length == 0) {
                return null;
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            try {
                GZIPInputStream ungzip = new GZIPInputStream(in);
                byte[] buffer = new byte[256];
                int n;
                while ((n = ungzip.read(buffer)) >= 0) {
                    out.write(buffer, 0, n);
                }
                return out.toString(StandardCharsets.UTF_8.name());
            } catch (IOException e) {
                logger.error("Corrupt GZIP,Can Not Decompress !");
                return null;
            }
        }
    }
}

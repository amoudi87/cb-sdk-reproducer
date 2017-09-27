package com.couchbase.sdk.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SporadicIllegalReferenceCountExceptionTest {
    @BeforeClass
    public static void setUp() throws Exception {
    }

    @AfterClass
    public static void tearDown() throws Exception {
    }

    @Test
    public void test() throws Exception {
        // Call docker compose up. This will setup a 3 nodes spock cluster
        System.out.println("Starting test from (" + System.getProperty("user.dir") + ")");
        ProcessBuilder PB = new ProcessBuilder();
        run(PB.command("src/test/resources/scripts/docker-compose-up.sh").start());

        // We Ensure servers are up
        run(PB.command("src/test/resources/scripts/ensure-servers-up.sh").start());
        run(PB.command("src/test/resources/scripts/config-cluster.sh").start());

        // Teardown cluster
        run(PB.command("src/test/resources/scripts/docker-compose-down.sh").start());
    }

    private static String run(Process p) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Future<Integer> future =
                Executors.newSingleThreadExecutor().submit(() -> IOUtils.copy(p.getInputStream(), new OutputStream() {
                    @Override
                    public void write(int b) throws IOException {
                        baos.write(b);
                    }

                    @Override
                    public void flush() throws IOException {
                        baos.flush();
                    }

                    @Override
                    public void close() throws IOException {
                        baos.close();
                    }
                }));

        int status = p.waitFor();
        future.get();
        ByteArrayInputStream bisIn = new ByteArrayInputStream(baos.toByteArray());
        StringWriter writerIn = new StringWriter();
        IOUtils.copy(bisIn, writerIn, StandardCharsets.UTF_8);
        StringWriter writerErr = new StringWriter();
        IOUtils.copy(p.getErrorStream(), writerErr, StandardCharsets.UTF_8);
        StringBuffer stdOut = writerIn.getBuffer();
        p.destroy();
        return status == 0 ? stdOut.toString()
                : "{\"status\":" + status + ",\"out\":\"" + stdOut.toString() + "\",\"err\":\"" + writerErr.toString()
                        + "\"}";
    }
}

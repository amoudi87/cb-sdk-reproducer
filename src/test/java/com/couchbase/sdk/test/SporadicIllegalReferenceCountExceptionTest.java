package com.couchbase.sdk.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.couchbase.analytics.test.util.KvStore;
import com.couchbase.analytics.test.util.Loader;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.User;

@RunWith(Parameterized.class)
public class SporadicIllegalReferenceCountExceptionTest {

    private static final int REPREAT_TEST_COUNT = 10;
    private static final String DCP_USERNAME = "till";
    private static final String NAME = "the westmann";
    private static final String BUCKET_NAME = "gbook_users";
    private static final int AWAIT_TIMEOUT_SECONDS = 90;
    private static final long LIMIT = 10000000L;
    private static final long TIMEOUT = 10000L;

    @Parameterized.Parameters
    public static List<Object[]> data() {
        return Arrays.asList(new Object[REPREAT_TEST_COUNT][0]);
    }

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
        System.out.println(run(PB.command("src/test/resources/scripts/docker-compose-up.sh").start()));
        // We Ensure servers are up
        System.out.println(run(PB.command("src/test/resources/scripts/ensure-servers-up.sh").start()));
        System.out.println(run(PB.command("src/test/resources/scripts/config-cluster.sh").start()));
        // Set parameters
        KvStore kvStore = KvStore.SPOCK;
        String cbUsername = "Administrator";
        String cbPassword = "couchbase";
        List<String> cbNodes = Arrays.asList("couchbase1.host,couchbase2.host,couchbase3.host".split(","));
        boolean verbose = true;
        // Create loader
        Loader cbLoader = new Loader(cbNodes, cbUsername, cbPassword, verbose, kvStore);
        CouchbaseCluster cbCluster = cbLoader.getCluster();
        // Create user
        createAdmin(cbLoader, DCP_USERNAME, NAME, cbPassword);
        // Drop bucket if exists
        cbLoader.dropBucketIfExists(BUCKET_NAME);
        // Create bucket
        cbLoader.createBucket(BUCKET_NAME, cbPassword, 128, 0, true);
        // Load records
        Loader.ID_NAMES.clear();
        Loader.ID_NAMES.add("id");
        String[] files = { "src/test/resources/data/p1/gbook_users.json" };
        for (String file : files) {
            cbLoader.load(cbCluster, cbPassword, BUCKET_NAME, false, new File(file), LIMIT, TIMEOUT, verbose);
        }
        // add a binary doc
        cbLoader.upsertBinaryDocument(cbCluster, cbPassword, "binary", StandardCharsets.UTF_16.encode("Hello"),
                BUCKET_NAME, TIMEOUT);

        // Load more records
        files = new String[] { "src/test/resources/data/p2/gbook_users.json" };
        for (String file : files) {
            cbLoader.load(cbCluster, cbPassword, BUCKET_NAME, false, new File(file), LIMIT, TIMEOUT, verbose);
        }

        // Drop the bucket
        cbLoader.dropBucketIfExists(BUCKET_NAME);

        // Teardown cluster
        System.out.println(run(PB.command("src/test/resources/scripts/docker-compose-down.sh").start()));
    }

    private static void createAdmin(Loader cbLoader, String username, String name, String password) {
        // create a powerful user
        Cluster cbCluster = cbLoader.getCluster();
        List<User> users = cbLoader.getUsers(cbCluster, AWAIT_TIMEOUT_SECONDS);
        boolean found = false;
        for (User user : users) {
            if (Objects.equals(user.userId(), username)) {
                found = true;
            }
        }
        if (!found) {
            cbLoader.upsertUser(cbCluster, username, name, password);
        }
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

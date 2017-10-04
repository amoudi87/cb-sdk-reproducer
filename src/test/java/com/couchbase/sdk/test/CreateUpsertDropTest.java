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
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.protocol.HttpClientContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.couchbase.analytics.reproducer.util.CBTestEnvironmentProvider;
import com.couchbase.analytics.reproducer.util.KvStore;
import com.couchbase.analytics.reproducer.util.Loader;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.User;

public class CreateUpsertDropTest {
    private static final int REPREAT_TEST_COUNT = 100;
    private static final String DCP_USERNAME = "till";
    private static final String NAME = "the westmann";
    private static final String BUCKET_NAME = "gbook_users";
    private static final boolean VERBOSE = false;
    private static final int AWAIT_TIMEOUT_SECONDS = 100; // create admin user timeout
    private static final long LIMIT = 10000000L;
    private static final long TIMEOUT = 100000L; // 100s kv operation timeout
    private static final KvStore kvStore = KvStore.SPOCK;
    private static final String cbUsername = "Administrator";
    private static final String cbPassword = "couchbase";
    private static final List<String> cbNodes =
            Arrays.asList("couchbase1.host,couchbase2.host,couchbase3.host".split(","));
    private static Loader cbLoader;
    private static CouchbaseCluster cbCluster;
    private static HttpClientContext httpCtx;

    @BeforeClass
    public static void setUp() throws Exception {
        // Call docker compose up. This will setup a 3 nodes spock cluster
        System.out.println("Starting test from (" + System.getProperty("user.dir") + ")");
        ProcessBuilder PB = new ProcessBuilder();
        System.out.println(run(PB.command("src/test/resources/scripts/docker-compose-up.sh").start()));
        // We Ensure servers are up
        System.out.println(run(PB.command("src/test/resources/scripts/ensure-servers-up.sh").start()));
        System.out.println(run(PB.command("src/test/resources/scripts/config-cluster.sh").start()));
        // Create loader
        cbLoader = new Loader(cbNodes, cbUsername, cbPassword, VERBOSE, kvStore);
        cbCluster = cbLoader.getCluster();
        // Create user
        createAdmin(cbLoader, DCP_USERNAME, NAME, cbPassword);
        httpCtx = HttpTestUtils.createBasicAuthHttpCtx(cbNodes, Pair.of(cbUsername, cbPassword));
    }

    @AfterClass
    public static void tearDown() throws Exception {
        // Teardown cluster
        ProcessBuilder PB = new ProcessBuilder();
        System.out.println(run(PB.command("src/test/resources/scripts/docker-compose-down.sh").start()));
    }

    @Test
    public void test() throws Exception {
        int i = 0;
        try {
            for (; i < REPREAT_TEST_COUNT; i++) {
                // Drop bucket if exists
                cbLoader.dropBucketIfExists(BUCKET_NAME);
                //Ensure that bucket has been dropped
                // <IP>:<PORT>/pools/default/buckets/<BUCKET_NAME> should return 404
                int port = CBTestEnvironmentProvider.getEnvironment().bootstrapHttpDirectPort();
                HttpTestUtils.request(cbNodes.get(0), port, "/pools/default/buckets/" + BUCKET_NAME,
                        response -> response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND, httpCtx);
                // Create bucket
                cbLoader.createBucket(BUCKET_NAME, cbPassword, 128, 0, true);
                // Ensure that bucket has been created
                HttpTestUtils.request(cbNodes.get(0), port, "/pools/default/buckets/" + BUCKET_NAME,
                        response -> response.getStatusLine().getStatusCode() == HttpStatus.SC_OK, httpCtx);
                // Load records
                Loader.ID_NAMES.clear();
                Loader.ID_NAMES.add("id");
                String[] files = { "src/test/resources/data/p1/gbook_users.json" };
                int total = 0;
                for (String file : files) {
                    total += cbLoader.load(cbCluster, cbPassword, BUCKET_NAME, false, new File(file), LIMIT, TIMEOUT,
                            VERBOSE);
                }
                // add a binary doc
                cbLoader.upsertBinaryDocument(cbCluster, cbPassword, "binary", StandardCharsets.UTF_16.encode("Hello"),
                        BUCKET_NAME, TIMEOUT);
                total++;
                // Load more records
                files = new String[] { "src/test/resources/data/p2/gbook_users.json" };
                for (String file : files) {
                    total += cbLoader.load(cbCluster, cbPassword, BUCKET_NAME, false, new File(file), LIMIT, TIMEOUT,
                            VERBOSE);
                }
                // Poll bucket info for validation
                final long roundTotal = total;
                HttpTestUtils.poll(cbNodes.get(0), port, "/pools/default/buckets/" + BUCKET_NAME,
                        new Predicate<HttpResponse>() {
                            @Override
                            public boolean test(HttpResponse response) {
                                if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                                    return false;
                                }
                                try {
                                    String json = HttpTestUtils.toString(response);
                                    return HttpTestUtils.check(json, "basicStats.itemCount", roundTotal);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                    return false;
                                }
                            }
                        }, httpCtx, 5, 1, TimeUnit.SECONDS);
                // Drop the bucket
                cbLoader.dropBucketIfExists(BUCKET_NAME);
                // Ensure that bucket has been dropped
                HttpTestUtils.request(cbNodes.get(0), port, "/pools/default/buckets/" + BUCKET_NAME,
                        response -> response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND, httpCtx);
                System.out.println("====================================");
                System.out.println("====================================");
                System.out.println("Completed " + (i + 1) + " rounds successfully");
                System.out.println("====================================");
                System.out.println("====================================");
            }
        } finally {
            if (i < REPREAT_TEST_COUNT) {
                System.err.println("Succeeded " + i + " times before it fails");
            }
        }
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

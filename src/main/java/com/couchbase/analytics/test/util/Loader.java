/*
 * Copyright 2016-2017 Couchbase, Inc.
 */
package com.couchbase.analytics.test.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.mutable.MutableLong;

import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.cluster.AuthDomain;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings.Builder;
import com.couchbase.client.java.cluster.User;
import com.couchbase.client.java.cluster.UserRole;
import com.couchbase.client.java.cluster.UserSettings;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.error.TemporaryFailureException;

public class Loader {
    private static final Logger LOGGER = Logger.getLogger(Loader.class.getName());
    public static final int RECORDS_PER_DOT = 1000;
    public static final int DOTS_PER_LINE = 80;
    public static final Set<String> ID_NAMES = new HashSet<>();
    private static final long MAX_TIMEOUT = TimeUnit.SECONDS.toMillis(120);

    private final CouchbaseCluster cluster;
    private final ClusterManager cm;
    public final List<String> nodes;
    public final boolean verbose;
    public final String username;
    public final String password;
    public final long timeout;

    public String bucketname;
    private final KvStore kvStore;

    public Loader(List<String> nodes, String username, String password, boolean verbose, KvStore kvStore) {
        this(nodes, username, password, verbose, kvStore, DefaultCoreEnvironment.BOOTSTRAP_HTTP_DIRECT_PORT,
                DefaultCoreEnvironment.BOOTSTRAP_CARRIER_DIRECT_PORT);
    }

    public Loader(List<String> nodes, String username, String password, boolean verbose, KvStore kvStore,
            int clusterMgmtPort, int kvPort) {
        this.nodes = nodes;
        this.verbose = verbose;
        this.username = username;
        this.password = password;
        CouchbaseEnvironment env = CBTestEnvironmentProvider.getEnvironment(clusterMgmtPort, kvPort);
        this.timeout = env.kvTimeout();
        this.cluster = CouchbaseCluster.create(env, nodes);
        this.kvStore = kvStore;
        this.cm = cluster.clusterManager(username, password);
        if (kvStore == KvStore.SPOCK) {
            cluster.authenticate(username, password);
        }
    }

    public void load(String filename, long limit, boolean flushBeforeLoad) throws IOException, InterruptedException {
        final File file = new File(filename);
        if (!file.exists()) {
            throw new FileNotFoundException(file.getAbsolutePath());
        }
        if (username != null && password != null) {
            createBucket(bucketname, password, 200, 0, true);
        }
        load(cluster, password, bucketname, flushBeforeLoad, file, limit, timeout, verbose);
    }

    public void load(Cluster cluster, String password, String bucketName, boolean flushBeforeLoad, File file,
            long limit, long timeout, boolean verbose) throws IOException, InterruptedException {
        Bucket bucket = kvStore == KvStore.SPOCK || password == null ? cluster.openBucket(bucketName)
                : cluster.openBucket(bucketName, password);
        if (flushBeforeLoad && !bucket.bucketManager().flush()) {
            throw new IOException("Could not flush " + bucketName);
        }
        parse(file, limit, bucket, verbose, timeout);
        if (!bucket.close()) {
            throw new IOException("Could not close " + bucketName);
        }
    }

    public List<String> getBuckets() {
        List<String> bucketNames = new ArrayList<>();
        LOGGER.info("Getting buckets...");
        for (BucketSettings bs : cm.getBuckets()) {
            bucketNames.add(bs.name());
        }
        LOGGER.info("Got buckets: " + bucketNames);
        return bucketNames;
    }

    public void dropBucketIfExists(String bucketName) {
        if (cm.hasBucket(bucketName)) {
            dropBucket(bucketName);
        }
    }

    public void dropBucket(String bucketName) {
        LOGGER.info("Dropping bucket " + bucketName + " with ClusterManager.removeBucket ...");
        boolean result = cm.removeBucket(bucketName);
        LOGGER.log(result ? Level.INFO : Level.WARNING,
                "Removal of bucket " + bucketName + " returned " + (result ? "successful" : "unsuccessful"));
        /*
         * TODO(amoudi): per DCP team this hasBucket call is insuffient to determine complete removal
         * For now, we will always wait 3 seconds. later, we can do something more involved.
         * Basically, we need to attempt to open the bucket and get an authentication failure
         */
        try {
            LOGGER.info("Waiting for bucket " + bucketName + " to be dropped");
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            LOGGER.log(Level.WARNING, "Interrupted while timed waiting", e);
        }
        LOGGER.info("Bucket " + bucketName + " dropped");
    }

    public BucketSettings createBucket(String bucketName, String password, int quotaMB, int replicas,
            boolean enableFlush) throws IOException {
        if (!cm.hasBucket(bucketName)) {
            try {
                Builder builder = new Builder().enableFlush(enableFlush).name(bucketName).password(password)
                        .quota(quotaMB).replicas(replicas);
                LOGGER.info("Creating bucket " + bucketName);
                cm.insertBucket(builder.build());
                // For now, we will always wait 3 seconds
                Thread.sleep(3000);
            } catch (Exception e) {
                throw new IOException("could not create bucket " + bucketName, e);
            }
        }
        LOGGER.info("Bucket " + bucketName + " created");
        return cm.getBucket(bucketName);
    }

    public void flushBucket(String bucketName, String password) throws IOException {
        Bucket bucket = kvStore == KvStore.SPOCK || password == null ? cluster.openBucket(bucketName)
                : cluster.openBucket(bucketName, password);
        if (!bucket.bucketManager().flush()) {
            throw new IOException("Could not flush " + bucketName);
        }
        if (!bucket.close()) {
            throw new IOException("Could not close " + bucketName);
        }
    }

    public void parse(File file, long limit, Bucket bucket, boolean verbose, long timeout)
            throws IOException, InterruptedException {
        LOGGER.info("+++ load start (" + bucket.name() + ") +++");
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            long count = 0;
            for (String line; (line = br.readLine()) != null;) {
                if (++count > limit) {
                    break;
                }
                JsonObject obj = JsonObject.fromJson(line);
                boolean upserted = false;
                for (String idName : ID_NAMES) {
                    if (!upserted && obj.containsKey(idName)) {
                        String id = String.valueOf(obj.get(idName));
                        JsonDocument doc = JsonDocument.create(id, obj);
                        MutableLong dynamicTimeout = new MutableLong(timeout);
                        while (!upserted) {
                            upserted = upsert(bucket, doc, dynamicTimeout);
                        }
                        if (verbose) {
                            LOGGER.info("Upserted " + doc);
                        } else {
                            progress(count, '.');
                        }
                    }
                }
                if (!upserted) {
                    if (verbose) {
                        LOGGER.info("No key found in " + obj);
                    } else {
                        progress(count, '-');
                    }
                }
            }
        } finally {
            LOGGER.info("+++ load end (" + bucket.name() + ") +++");
        }
    }

    public void parse(File file, long limit, Bucket bucket) throws IOException, InterruptedException {
        parse(file, limit, bucket, verbose, timeout);
    }

    public boolean upsert(Bucket bucket, Document<?> doc, MutableLong timeout) throws InterruptedException {
        try {
            bucket.upsert(doc, timeout.getValue(), TimeUnit.MILLISECONDS);
            return true;
        } catch (RuntimeException e) {
            if (e instanceof TemporaryFailureException || e.getCause() instanceof TimeoutException) {
                final long oldTimeout = timeout.getValue();
                final long newTimeout = oldTimeout * 2;
                if (newTimeout > MAX_TIMEOUT) {
                    LOGGER.log(Level.WARNING, "Max timeout exceeded; failing on temp / timeout exception", e);
                    throw e;
                }
                timeout.setValue(newTimeout);
                LOGGER.info("+++ caught " + e + "; new timeout " + newTimeout + "(" + bucket.name() + ") +++");
            } else {
                throw e;
            }
        }
        return false;
    }

    public void progress(long count, char c) {
        if (count % RECORDS_PER_DOT == 0) {
            System.err.print(c);
            System.err.flush();
            if (count % (RECORDS_PER_DOT * DOTS_PER_LINE) == 0) {
                System.err.println();
            }
        }
    }

    private static void usage(String msg) {
        String usage = msg + "\nParameters: [options] <filename>\n" + "Options:\n"
                + "  -l <num>        (limit - number of records to load)\n"
                + "  -b <bucketname> (default: \"default\")\n" + "  -f              (flush bucket before loading)\n"
                + "  -h <host>       (default: \"localhost\")\n"
                + "  -k <fieldname>  (key field, can occur more than once, first match is chosen)\n"
                + "  -u <username>   (admin user)\n" + "  -p <password>   (admin password)\n"
                + "  -v              verbose\n" + "  -s              spock\n";
        System.err.println(usage);
        System.exit(1);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length == 0) {
            usage("no arguments");
        }

        long limit = Long.MAX_VALUE;
        String bucket = "default";
        List<String> nodes = new ArrayList<>();
        nodes.add("localhost");
        String filename = "";
        boolean flushBeforeLoad = false;
        boolean verbose = false;
        String username = null;
        String password = null;
        KvStore kvStore = KvStore.WATSON;
        int i = 0;
        while (i < args.length) {
            String arg = args[i];
            if (arg.startsWith("-")) {
                if (i + 1 >= args.length) {
                    usage("missing value for option " + arg);
                }
                switch (arg) {
                    case "-l":
                        limit = Long.valueOf(args[++i]);
                        break;
                    case "-b":
                        bucket = args[++i];
                        break;
                    case "-h":
                        nodes.add(args[++i]);
                        break;
                    case "-k":
                        ID_NAMES.add(args[++i]);
                        break;
                    case "-f":
                        flushBeforeLoad = true;
                        break;
                    case "-v":
                        verbose = true;
                        break;
                    case "-u":
                        username = args[++i];
                        break;
                    case "-p":
                        password = args[++i];
                        break;
                    case "-spock":
                        kvStore = KvStore.SPOCK;
                        break;
                    default:
                        usage("unknown option " + arg);
                }
            } else {
                if (!filename.equals("")) {
                    usage("more than 1 filename given");
                }
                filename = arg;
            }
            ++i;
        }
        if (ID_NAMES.isEmpty()) {
            usage("need at least 1 key field");
        }
        if (filename.equals("")) {
            usage("no filename given");
        }
        int clusterMgmtPort = DefaultCoreEnvironment.BOOTSTRAP_HTTP_DIRECT_PORT;
        char delimiter = ':';
        for (String node : nodes) {
            int index = node.indexOf(delimiter);
            if (index >= 0) {
                clusterMgmtPort = Integer.parseInt(node.substring(index + 1));
            }
        }
        Loader loader = new Loader(nodes, username, password, verbose, kvStore, clusterMgmtPort,
                DefaultCoreEnvironment.BOOTSTRAP_CARRIER_DIRECT_PORT);
        loader.setBucketName(bucket);
        loader.load(filename, limit, flushBeforeLoad);
    }

    public void setBucketName(String bucket) {
        this.bucketname = bucket;
    }

    public CouchbaseCluster getCluster() {
        return cluster;
    }

    /**
     * Delete a document from a bucket using key
     *
     * @param cbCluster
     * @param cbPassword
     * @param bucketName
     * @param key
     * @param timeout
     * @param verbose
     */
    public void delete(Cluster cbCluster, String cbPassword, String bucketName, String[] keys, long timeout,
            boolean verbose) {
        Bucket bucket = kvStore == KvStore.SPOCK || password == null ? cluster.openBucket(bucketName)
                : cluster.openBucket(bucketName, password);
        try {
            for (String key : keys) {
                bucket.remove(key, PersistTo.MASTER);
            }
        } finally {
            bucket.close();
        }
    }

    public List<User> getUsers(Cluster cbCluster, long timeout) {
        ClusterManager clusterMgr = cbCluster.clusterManager();
        return clusterMgr.getUsers(AuthDomain.LOCAL);
    }

    public void upsertUser(Cluster cbCluster, String username, String name, String password) {
        ClusterManager clusterMgr = cbCluster.clusterManager();
        UserSettings us = UserSettings.build().name(name).password(password)
                .roles(Collections.singletonList(new UserRole("admin")));
        clusterMgr.upsertUser(AuthDomain.LOCAL, username, us);
    }

    public Bucket open(Cluster cluster, String password, String bucketName) {
        return kvStore == KvStore.SPOCK || password == null ? cluster.openBucket(bucketName)
                : cluster.openBucket(bucketName, password);
    }

    public void delete(Bucket bucket, String[] keys) {
        for (String key : keys) {
            bucket.remove(key, PersistTo.MASTER);
        }
    }

    public void upsertStringDocument(Cluster cluster, String password, String key, String value, String bucketName,
            long timeout) throws IOException, InterruptedException {
        Bucket bucket = kvStore == KvStore.SPOCK || password == null ? cluster.openBucket(bucketName)
                : cluster.openBucket(bucketName, password);
        upsertStringDocument(key, value, bucket, timeout);
        if (!bucket.close()) {
            throw new IOException("Could not close " + bucketName);
        }
    }

    public void upsertStringDocument(String key, String value, Bucket bucket, long timeout)
            throws IOException, InterruptedException {
        boolean upserted = false;
        StringDocument doc = StringDocument.create(key, value);
        MutableLong dynamicTimeout = new MutableLong(timeout);
        while (!upserted) {
            upserted = upsert(bucket, doc, dynamicTimeout);
        }
    }

    public void upsertBinaryDocument(String key, ByteBuffer value, Bucket bucket, long timeout)
            throws IOException, InterruptedException {
        boolean upserted = false;
        ByteBuf buf = Unpooled.wrappedBuffer(value);
        try {
            BinaryDocument doc = BinaryDocument.create(key, buf);
            MutableLong dynamicTimeout = new MutableLong(timeout);
            while (!upserted) {
                buf.retain();
                upserted = upsert(bucket, doc, dynamicTimeout);
            }
        } finally {
            buf.release();
        }
    }

    public void upsertBinaryDocument(Cluster cluster, String password, String key, ByteBuffer value, String bucketName,
            long timeout) throws IOException, InterruptedException {
        Bucket bucket = kvStore == KvStore.SPOCK || password == null ? cluster.openBucket(bucketName)
                : cluster.openBucket(bucketName, password);
        upsertBinaryDocument(key, value, bucket, timeout);
        if (!bucket.close()) {
            throw new IOException("Could not close " + bucketName);
        }
    }

    public void upsert(String key, String value, Bucket bucket, long timeout) throws IOException, InterruptedException {
        boolean upserted = false;
        JsonObject obj = JsonObject.fromJson(value);
        JsonDocument doc = JsonDocument.create(key, obj);
        MutableLong dynamicTimeout = new MutableLong(timeout);
        while (!upserted) {
            upserted = upsert(bucket, doc, dynamicTimeout);
        }
    }
}

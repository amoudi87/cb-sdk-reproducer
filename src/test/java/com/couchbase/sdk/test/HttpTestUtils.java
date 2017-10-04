package com.couchbase.sdk.test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.ValidationException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.apache.http.protocol.HttpContext;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class HttpTestUtils {

    private static final Logger LOGGER = Logger.getLogger(CreateUpsertDropTest.class.getName());
    private static final long MAX_URL_LENGTH = 2000L;

    public void testJsonChecker() throws Exception {
        String content =
                "{\"name\":\"travel-sample\",\"bucketType\":\"membase\",\"authType\":\"sasl\",\"proxyPort\":0,\""
                        + "uri\":\"/pools/default/buckets/travel-sample?bucket_uuid=5b1f0137a0242b63b01ab8dedb8a5b27\",\"streamingUri"
                        + "\":\"/pools/default/bucketsStreaming/travel-sample?bucket_uuid=5b1f0137a0242b63b01ab8dedb8a5b27\""
                        + ",\"localRandomKeyUri\":\"/pools/default/buckets/travel-sample/localRandomKey\",\"controllers\":{\"compactAll\""
                        + ":\"/pools/default/buckets/travel-sample/controller/compactBucket\",\"compactDB"
                        + "\":\"/pools/default/buckets/travel-sample/controller/compactDatabases\",\"purgeDeletes\":\"/pools/"
                        + "default/buckets/travel-sample/controller/unsafePurgeBucket\",\"startRecovery\":\"/pools/default/buckets"
                        + "/travel-sample/controller/startRecovery\"},\"nodes\":[{\"couchApiBaseHTTPS\":\"https://127.0.0.1:19502/"
                        + "travel-sample%2B5b1f0137a0242b63b01ab8dedb8a5b27\",\"couchApiBase\":\"http://127.0.0.1:9502/travel-sample"
                        + "%2B5b1f0137a0242b63b01ab8dedb8a5b27\",\"systemStats\":{\"cpu_utilization_rate\":26.24378109452736,\"swap_total"
                        + "\":2147483648,\"swap_used\":583270400,\"mem_total\":17179869184,\"mem_free\":6275207168},\"interestingStats"
                        + "\":{\"cmd_get\":0,\"couch_docs_actual_disk_size\":90661988,\"couch_docs_data_size\":84970496,\""
                        + "couch_spatial_data_size\":0,\"couch_spatial_disk_size\":0,\"couch_views_actual_disk_size\":0,\"couch_views_"
                        + "data_size\":0,\"curr_items\":15824,\"curr_items_tot\":31591,\"ep_bg_fetched\":0,\"get_hits\":0,\"mem_used\":"
                        + "73138912,\"ops\":0,\"vb_active_num_non_resident\":0,\"vb_replica_curr_items\":15767},\"uptime\":\"7069\",\""
                        + "memoryTotal\":17179869184,\"memoryFree\":6275207168,\"mcdMemoryReserved\":13107,\"mcdMemoryAllocated\":13107"
                        + ",\"replication\":1,\"clusterMembership\":\"active\",\"recoveryType\":\"none\",\"status\":\"healthy\",\"otpNode"
                        + "\":\"n_2@127.0.0.1\",\"hostname\":\"127.0.0.1:9002\",\"clusterCompatibility\":327680,\"version\":\"5.0.0-0000-"
                        + "enterprise\",\"os\":\"x86_64-apple-darwin13.4.0\",\"ports\":{\"sslProxy\":11990,\"httpsMgmt\":19002,\"httpsCAPI"
                        + "\":19502,\"proxy\":12005,\"direct\":12004},\"services\":[\"kv\"]},{\"couchApiBaseHTTPS\":\"https://192.168.0.3:"
                        + "19500/travel-sample%2B5b1f0137a0242b63b01ab8dedb8a5b27\",\"couchApiBase\":\"http://192.168.0.3:9500/travel-sample"
                        + "%2B5b1f0137a0242b63b01ab8dedb8a5b27\",\"systemStats\":{\"cpu_utilization_rate\":23.31670822942644,\"swap_total"
                        + "\":2147483648,\"swap_used\":583270400,\"mem_total\":17179869184,\"mem_free\":6271184896},\"interestingStats\""
                        + ":{\"cmd_get\":0,\"couch_docs_actual_disk_size\":87143678,\"couch_docs_data_size\":81460224,\"couch_spatial_data_"
                        + "size\":0,\"couch_spatial_disk_size\":0,\"couch_views_actual_disk_size\":0,\"couch_views_data_size\":0,\"curr_items"
                        + "\":15767,\"curr_items_tot\":31591,\"ep_bg_fetched\":0,\"get_hits\":0,\"mem_used\":74065336,\"ops\":0,\"vb_active_num"
                        + "_non_resident\":0,\"vb_replica_curr_items\":15824},\"uptime\":\"7072\",\"memoryTotal\":17179869184,\"memoryFree\":"
                        + "6271184896,\"mcdMemoryReserved\":13107,\"mcdMemoryAllocated\":13107,\"replication\":1,\"clusterMembership\":"
                        + "\"active\",\"recoveryType\":\"none\",\"status\":\"healthy\",\"otpNode\":\"n_0@192.168.0.3\",\"thisNode\":"
                        + "true,\"hostname\":\"192.168.0.3:9000\",\"clusterCompatibility\":327680,\"version\":\"5.0.0-0000-enterprise\""
                        + ",\"os\":\"x86_64-apple-darwin13.4.0\",\"ports\":{\"sslProxy\":11998,\"httpsMgmt\":19000,\"httpsCAPI\":19500"
                        + ",\"proxy\":12001,\"direct\":12000},\"services\":[\"cbas\",\"kv\"]}],\"stats\":{\"uri\":\"/pools/default/buckets"
                        + "/travel-sample/stats\",\"directoryURI\":\"/pools/default/buckets/travel-sample/statsDirectory\",\"nodeStatsList"
                        + "URI\":\"/pools/default/buckets/travel-sample/nodes\"},\"nodeLocator\":\"vbucket\",\"saslPassword\":\"32251df2ab"
                        + "0bee564d677c6f9a05ab6f\",\"ddocs\":{\"uri\":\"/pools/default/buckets/travel-sample/ddocs\"},\"replicaIndex\":false"
                        + ",\"autoCompactionSettings\":false,\"uuid\":\"5b1f0137a0242b63b01ab8dedb8a5b27\",\"vBucketServerMap\":{\"hashAlgorithm"
                        + "\":\"CRC\",\"numReplicas\":1,\"serverList\":[\"192.168.0.3:12000\",\"127.0.0.1:12004\"],\"vBucketMap\":"
                        + "[[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],"
                        + "[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[0,1],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],"
                        + "[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0],[1,0]]},\""
                        + "replicaNumber\":1,\"threadsNumber\":3,\"quota\":{\"ram\":209715200,\"rawRAM\":104857600},\"basicStats\":"
                        + "{\"quotaPercentUsed\":70.19245529174805,\"opsPerSec\":0,\"diskFetches\":0,\"itemCount\":31591,\"diskUsed\""
                        + ":177805666,\"dataUsed\":166430720,\"memUsed\":147204248,\"vbActiveNumNonResident\":0},\"evictionPolicy\":"
                        + "\"valueOnly\",\"conflictResolutionType\":\"seqno\",\"bucketCapabilitiesVer\":\"\",\"bucketCapabilities\":"
                        + "[\"xattr\",\"dcp\",\"cbhello\",\"touch\",\"couchapi\",\"cccp\",\"xdcrCheckpointing\",\"nodesExt\"]}";
        System.out.println(check("{\"name\":\"value\"}", "name", "value"));
        System.out.println(check(content, "basicStats.itemCount", 31591));
    }

    public static boolean check(String json, String path, String value) {
        try {
            // read the json and produce a tree
            JsonNode node = getNode(json, path);
            if (node == null) {
                return false;
            }
            return node.asText().equals(value);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed during json check", e);
            return false;
        }
    }

    public static boolean check(String json, String path, long value) {
        try {
            // read the json and produce a tree
            JsonNode node = getNode(json, path);
            if (node == null) {
                LOGGER.log(Level.WARNING, "Expected:" + value + ", Field " + path + " not found in:" + json);
                return false;
            }
            if (node.asLong() == value) {
                return true;
            } else {
                LOGGER.log(Level.WARNING, "Expected:" + value + ", Found:" + node.asLong());
                return false;
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed during json check", e);
            return false;
        }
    }

    private static JsonNode getNode(String json, String path)
            throws JsonParseException, JsonMappingException, IOException {
        String[] tokens = path == null ? new String[0] : path.contains(".") ? path.split("\\.") : new String[] { path };
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readValue(json, JsonNode.class);
        for (int i = 0; i < tokens.length; i++) {
            node = node.get(tokens[i]);
            if (node == null) {
                return null;
            }
        }
        return node;
    }

    public static HttpResponse request(String host, int port, String path, HttpContext ctx) throws Exception {
        // Create a method instance.
        HttpUriRequest request = RequestBuilder.get("http://" + host + ":" + port + path)
                .setVersion(HttpVersion.HTTP_1_1).setCharset(StandardCharsets.UTF_8).build();
        return executeAndCheckHttpRequest(request, ctx);
    }

    public static HttpResponse request(String host, int port, String path, Predicate<HttpResponse> validator,
            HttpContext ctx) throws Exception {
        HttpResponse response = executeAndCheckHttpRequest(
                RequestBuilder.get("http://" + host + ":" + port + path).build(), validator, ctx);
        return response;
    }

    public static String toString(HttpResponse response) throws IOException {
        InputStream inputStream = response.getEntity().getContent();
        return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }

    public static void poll(String host, int port, String path, Predicate<HttpResponse> validator,
            HttpClientContext ctx, int attempts, int waitBetweenAttempts, TimeUnit timeUnit) throws Exception {
        boolean succeeded = false;
        while (!succeeded && attempts > 0) {
            attempts--;
            try {
                request(host, port, path, validator, ctx);
                return;
            } catch (ValidationException ve) {
                if (attempts == 0) {
                    throw ve;
                }
                Thread.sleep(timeUnit.toMillis(waitBetweenAttempts));
            }
        }
    }

    public InputStream executeQuery(String str, String fmt, URI uri, List<Pair<String, String>> params, HttpContext ctx)
            throws Exception {
        HttpUriRequest method = constructHttpMethod(str, uri, "query", false, params);
        // Set accepted output response type
        method.setHeader("Accept", fmt);
        HttpResponse response = executeAndCheckHttpRequest(method, ctx);
        return response.getEntity().getContent();
    }

    public static HttpResponse executeAndCheckHttpRequest(HttpUriRequest method, HttpContext ctx) throws Exception {
        return checkResponse(executeHttpRequest(method, ctx),
                response -> response.getStatusLine().getStatusCode() == HttpStatus.SC_OK);
    }

    public static HttpResponse executeHttpRequest(HttpUriRequest method, HttpContext ctx) throws Exception {
        HttpClient client = HttpClients.custom().setRetryHandler(StandardHttpRequestRetryHandler.INSTANCE).build();
        try {
            return client.execute(method, ctx);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            throw e;
        }
    }

    public static HttpResponse checkResponse(HttpResponse httpResponse, Predicate<HttpResponse> responseValidator)
            throws Exception {
        if (!responseValidator.test(httpResponse)) {
            throw new ResponseValidationException(httpResponse);
        }
        return httpResponse;
    }

    public InputStream executeQueryService(String str, URI uri, String fmt, HttpContext ctx) throws Exception {
        return executeQueryService(str, fmt, uri, new ArrayList<>(), false, ctx);
    }

    public InputStream executeQueryService(String str, String fmt, URI uri, List<Pair<String, String>> params,
            boolean jsonEncoded, HttpContext ctx) throws Exception {
        return executeQueryService(str, fmt, uri, params, jsonEncoded, null, false, ctx);
    }

    public InputStream executeQueryService(String str, String fmt, URI uri, List<Pair<String, String>> params,
            boolean jsonEncoded, Predicate<HttpResponse> responseValidator, HttpContext ctx) throws Exception {
        return executeQueryService(str, fmt, uri, params, jsonEncoded, responseValidator, false, ctx);
    }

    public InputStream executeQueryService(String str, String fmt, URI uri, List<Pair<String, String>> params,
            boolean jsonEncoded, Predicate<HttpResponse> responseValidator, boolean cancellable, HttpContext ctx)
            throws Exception {
        final List<Pair<String, String>> newParams = upsertParam(params, "format", fmt);
        HttpUriRequest method = jsonEncoded ? constructPostMethodJson(str, uri, "statement", newParams)
                : constructPostMethodUrl(str, uri, "statement", newParams);
        // Set accepted output response type
        method.setHeader("Accept", "application/json");
        HttpResponse response = executeHttpRequest(method, ctx);
        if (responseValidator != null) {
            checkResponse(response, responseValidator);
        }
        return response.getEntity().getContent();
    }

    private HttpUriRequest constructHttpMethod(String statement, URI uri, String stmtParam, boolean postStmtAsParam,
            List<Pair<String, String>> otherParams) throws URISyntaxException {
        if (statement.length() + uri.toString().length() < MAX_URL_LENGTH) {
            // Use GET for small-ish queries
            return constructGetMethod(uri, upsertParam(otherParams, stmtParam, statement));
        } else {
            // Use POST for bigger ones to avoid 413 FULL_HEAD
            String stmtParamName = (postStmtAsParam ? stmtParam : null);
            return constructPostMethodUrl(statement, uri, stmtParamName, otherParams);
        }
    }

    private HttpUriRequest constructGetMethod(URI endpoint, List<Pair<String, String>> params) {
        RequestBuilder builder = RequestBuilder.get(endpoint);
        for (Pair<String, String> param : params) {
            builder.addParameter(param.getKey(), param.getValue());
        }
        builder.setCharset(StandardCharsets.UTF_8);
        return builder.build();
    }

    private HttpUriRequest buildRequest(String method, URI uri, List<Pair<String, String>> params) {
        RequestBuilder builder = RequestBuilder.create(method);
        builder.setUri(uri);
        for (Pair<String, String> param : params) {
            builder.addParameter(param.getKey(), param.getValue());
        }
        builder.setCharset(StandardCharsets.UTF_8);
        return builder.build();
    }

    private HttpUriRequest buildRequest(String method, URI uri, String fmt, List<Pair<String, String>> params) {
        HttpUriRequest request = buildRequest(method, uri, params);
        // Set accepted output response type
        request.setHeader("Accept", fmt);
        return request;
    }

    protected HttpUriRequest constructPostMethodUrl(String statement, URI uri, String stmtParam,
            List<Pair<String, String>> otherParams) {
        RequestBuilder builder = RequestBuilder.post(uri);
        if (stmtParam != null) {
            for (Pair<String, String> param : upsertParam(otherParams, stmtParam, statement)) {
                builder.addParameter(param.getKey(), param.getValue());
            }
            builder.addParameter(stmtParam, statement);
        } else {
            // this seems pretty bad - we should probably fix the API and not the client
            builder.setEntity(new StringEntity(statement, StandardCharsets.UTF_8));
        }
        builder.setCharset(StandardCharsets.UTF_8);
        return builder.build();
    }

    protected HttpUriRequest constructPostMethodJson(String statement, URI uri, String stmtParam,
            List<Pair<String, String>> otherParams) {
        if (stmtParam == null) {
            throw new NullPointerException("Statement parameter required.");
        }
        RequestBuilder builder = RequestBuilder.post(uri);
        ObjectMapper om = new ObjectMapper();
        ObjectNode content = om.createObjectNode();
        for (Pair<String, String> param : upsertParam(otherParams, stmtParam, statement)) {
            content.put(param.getKey(), param.getValue());
        }
        try {
            builder.setEntity(new StringEntity(om.writeValueAsString(content), ContentType.APPLICATION_JSON));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        builder.setCharset(StandardCharsets.UTF_8);
        return builder.build();
    }

    public InputStream executeJSONGet(String fmt, URI uri, HttpContext ctx) throws Exception {
        return executeJSON(fmt, "GET", uri, Collections.emptyList(), ctx);
    }

    public InputStream executeJSONGet(String fmt, URI uri, List<Pair<String, String>> params,
            Predicate<HttpResponse> responseValidator, HttpContext ctx) throws Exception {
        return executeJSON(fmt, "GET", uri, params, responseValidator, ctx);
    }

    public InputStream executeJSON(String fmt, String method, URI uri, List<Pair<String, String>> params,
            HttpContext ctx) throws Exception {
        return executeJSON(fmt, method, uri, params,
                response -> response.getStatusLine().getStatusCode() == HttpStatus.SC_OK, ctx);
    }

    public InputStream executeJSON(String fmt, String method, URI uri, Predicate<HttpResponse> responseValidator,
            HttpContext ctx) throws Exception {
        return executeJSON(fmt, method, uri, Collections.emptyList(), responseValidator, ctx);
    }

    public InputStream executeJSON(String fmt, String method, URI uri, List<Pair<String, String>> params,
            Predicate<HttpResponse> responseValidator, HttpContext ctx) throws Exception {
        HttpUriRequest request = buildRequest(method, uri, fmt, params);
        HttpResponse response = executeAndCheckHttpRequest(request, responseValidator, ctx);
        return response.getEntity().getContent();
    }

    public static HttpResponse executeAndCheckHttpRequest(HttpUriRequest method,
            Predicate<HttpResponse> responseValidator, HttpContext ctx) throws Exception {
        return checkResponse(executeHttpRequest(method, ctx), responseValidator);
    }

    // To execute Update statements
    // Insert and Delete statements are executed here
    public void executeUpdate(String str, URI uri, HttpContext ctx) throws Exception {
        // Create a method instance.
        HttpUriRequest request =
                RequestBuilder.post(uri).setEntity(new StringEntity(str, StandardCharsets.UTF_8)).build();

        // Execute the method.
        executeAndCheckHttpRequest(request, ctx);
    }

    public static HttpClientContext createBasicAuthHttpCtx(List<String> hosts, Pair<String, String> credentials) {
        List<HttpHost> authHosts = new ArrayList<>();
        for (String host : hosts) {
            authHosts.add(HttpHost.create(host));
        }
        final CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(credentials.getLeft(), credentials.getRight()));
        final AuthCache authCache = new BasicAuthCache();
        if (authHosts != null) {
            for (HttpHost authHost : authHosts) {
                authCache.put(authHost, new BasicScheme());
            }
        }
        final HttpClientContext ctx = HttpClientContext.create();
        ctx.setCredentialsProvider(credsProvider);
        ctx.setAuthCache(authCache);
        return ctx;
    }

    protected List<Pair<String, String>> upsertParam(List<Pair<String, String>> params, String name, String value) {
        boolean replaced = false;
        List<Pair<String, String>> result = new ArrayList<>();
        for (Pair<String, String> param : params) {
            Pair<String, String> newParam;
            if (name.equals(param.getKey())) {
                newParam = Pair.of(name, value);
                replaced = true;
            } else {
                newParam = param;
            }
            result.add(newParam);
        }
        if (!replaced) {
            Pair<String, String> newParam = Pair.of(name, value);
            result.add(newParam);
        }
        return result;
    }
}

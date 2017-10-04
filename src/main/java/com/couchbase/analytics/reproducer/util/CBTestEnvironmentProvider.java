/*
 * Copyright 2016-2017 Couchbase, Inc.
 */
package com.couchbase.analytics.reproducer.util;

import java.util.logging.Logger;

import org.apache.commons.lang3.mutable.MutableObject;

import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

public class CBTestEnvironmentProvider {
    private static final Logger LOGGER = Logger.getLogger(CBTestEnvironmentProvider.class.getName());
    private static final long AUTO_RELEASE_AFTER_MILLISECONDS = 75000L;
    private static final long ENV_TIMEOUT = 75000; // default max request time
    private static final MutableObject<State> state = new MutableObject<>(State.UNINITIALIZED);
    private static int CLUSTER_MGMT_PORT = DefaultCoreEnvironment.BOOTSTRAP_HTTP_DIRECT_PORT;
    private static int KV_PORT = DefaultCoreEnvironment.BOOTSTRAP_CARRIER_DIRECT_PORT;

    private enum State {
        UNINITIALIZED,
        SHARED,
        INITIALIZED,
        SHUTDOWN
    }

    private static class Holder {
        private static final CouchbaseEnvironment INSTANCE;

        private Holder() {
        }

        static {
            synchronized (state) {
                CouchbaseEnvironment instance;
                try {
                    Class<?> cbEnvProvider = Class.forName("com.couchbase.analytics.util.CBEnvironmentProvider");
                    instance = (CouchbaseEnvironment) cbEnvProvider.getMethod("getEnvironment").invoke(null);
                    state.setValue(State.SHARED);
                } catch (Exception e) {
                    LOGGER.info("Initializing Test Couchbase Environment");
                    instance = DefaultCouchbaseEnvironment.builder().connectTimeout(ENV_TIMEOUT)
                            .disconnectTimeout(ENV_TIMEOUT).kvTimeout(ENV_TIMEOUT)
                            .bootstrapHttpDirectPort(CLUSTER_MGMT_PORT).bootstrapCarrierDirectPort(KV_PORT)
                            .autoreleaseAfter(AUTO_RELEASE_AFTER_MILLISECONDS).build();
                    state.setValue(State.INITIALIZED);
                }
                INSTANCE = instance;
            }
        }

    }

    public static CouchbaseEnvironment getEnvironment(int clusterMgmtPort, int kvPort) {
        CLUSTER_MGMT_PORT = clusterMgmtPort;
        KV_PORT = kvPort;
        return Holder.INSTANCE;
    }

    public static CouchbaseEnvironment getEnvironment() {
        return Holder.INSTANCE;
    }

    public static void shutdownIfInitialized() {
        synchronized (state) {
            if (state.getValue() == State.INITIALIZED) {
                try {
                    LOGGER.info("Shutting down Couchbase Environment");
                    getEnvironment().shutdown();
                } finally {
                    state.setValue(State.SHUTDOWN);
                }
            }
        }
    }
}

package io.bouvier.thomas.flink.mastodon;

import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.mastodon4j.core.MastodonClient;
import org.mastodon4j.core.api.Accounts;
import org.mastodon4j.core.api.BaseMastodonApi;
import org.mastodon4j.core.api.EventStream;
import org.mastodon4j.core.api.Lists;
import org.mastodon4j.core.api.MastodonApi;
import org.mastodon4j.core.api.Statuses;
import org.mastodon4j.core.api.Streaming;
import org.mastodon4j.core.api.Timelines;
import org.mastodon4j.core.api.entities.AccessToken;
import org.mastodon4j.core.api.entities.Account;
import org.mastodon4j.core.api.entities.Event;
import org.mastodon4j.core.api.entities.MList;
import org.mastodon4j.core.api.entities.Status;
import org.mastodon4j.core.api.entities.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;

/**
 * Implementation of {@link SourceFunction} specialized to emit posts from Mastodon.
 */
public class MastodonSource extends RichSourceFunction<String> {

    private static final Logger LOG = LoggerFactory.getLogger(MastodonSource.class);

    private static final long serialVersionUID = 1L;

    // ----- Required property keys

    public static final String INSTANCE_STRING = "mastodon-source.instanceString";

    public static final String ACCESS_TOKEN = "mastodon-source.accessToken";

    // ----- Fields set by the constructor

    private final Properties properties;

    // ----- Runtime fields
    private Streaming client;
    private AccessToken accessToken;
    private transient Object waitLock;
    private transient boolean running = true;

    /**
     * Create {@link MastodonSource} for streaming.
     *
     * @param properties For the source
     */
    public MastodonSource(Properties properties) {
        checkProperty(properties, INSTANCE_STRING);
        checkProperty(properties, ACCESS_TOKEN);

        this.properties = properties;
    }

    private static void checkProperty(Properties p, String key) {
        if (!p.containsKey(key)) {
            throw new IllegalArgumentException("Required property '" + key + "' not set.");
        }
    }

    // ----- Source lifecycle

    @Override
    public void open(Configuration parameters) throws Exception {
        waitLock = new Object();
    }

    @Override
    public void run(final SourceContext<String> ctx) throws Exception {
        LOG.info("Initializing Mastodon Streaming API connection");

        accessToken = AccessToken.create(
                properties.getProperty(ACCESS_TOKEN)
        );

        client =
                MastodonClient.create(
                        properties.getProperty(INSTANCE_STRING),
                        accessToken).streaming();

        running = true;

        LOG.info("Mastodon Streaming API connection established successfully");

        // just wait now
        while (running) {
            synchronized (waitLock) {
                waitLock.wait(100L);
            }
        }
    }

    @Override
    public void close() {
        this.running = false;
        LOG.info("Closing source");
        /*
        if (client != null) {
            // client seems to be thread-safe
            client.stop();
        }
        */
        // leave main method
        synchronized (waitLock) {
            waitLock.notify();
        }
    }

    @Override
    public void cancel() {
        LOG.info("Cancelling Mastodon source");
        close();
    }
}

package org.apache.solr.cluster.events;

import java.util.concurrent.TimeoutException;

public interface VersionTracker {
    void increment();

    int waitForVersionChange(int currentVersion, int timeoutSec) throws InterruptedException, TimeoutException;
}

package org.apache.solr.cluster.events;

import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class VersionTrackerImpl implements VersionTracker {
    private int version = 0;

    @Override
    public synchronized void increment() {
        version++;
        this.notifyAll();
    }

    @Override
    public int waitForVersionChange(int currentVersion, int timeoutSec) throws InterruptedException, TimeoutException {
        TimeOut timeout = new TimeOut(timeoutSec, TimeUnit.SECONDS, TimeSource.NANO_TIME);
        int newVersion = currentVersion;
        while (! timeout.hasTimedOut()) {
            synchronized (this) {
                if ((newVersion = version) != currentVersion) {
                    break;
                }
                this.wait(timeout.timeLeft(TimeUnit.MILLISECONDS));
            }
        }
        if (newVersion < currentVersion) {
            // ArithmeticException? This means we overflowed
            throw new RuntimeException("Invalid version - went back! currentVersion=" + currentVersion +
                    " newVersion=" + newVersion);
        } else if (newVersion == currentVersion) {
            throw new TimeoutException("Timed out waiting for version change.");
        }

        return newVersion;
    }
}

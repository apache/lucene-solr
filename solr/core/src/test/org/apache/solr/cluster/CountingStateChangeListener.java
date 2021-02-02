/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cluster;

import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A counting StateChangeListener that will internally track how many times {@link #stateChanged()} has been called.
 * Consumers can compare the number of state change calls before and after an event to determine if they should proceed,
 * made simple with {@link #waitForVersionChange(int, int)} method.
 */
public class CountingStateChangeListener implements StateChangeListener {
    private int version = 0;

    @Override
    public synchronized void stateChanged() {
        version++;
        this.notifyAll();
    }

    /**
     * Given a last known number of state changes, wait for additional changes to come in. If no state changes have
     * occurred beyond the known value, this method will wait for additional changes to come in.
     * If the current number of change events is unknown to the caller, then this method can be called with <tt>-1</tt>
     * to return immediately with the number of events up to this point.
     * @param lastVersion the previous number of changes seen
     * @param timeoutSec how long to wait for additional changes to occur
     * @return the number of changes seen since initialization
     */
    public int waitForVersionChange(int lastVersion, int timeoutSec) throws InterruptedException, TimeoutException {
        TimeOut timeout = new TimeOut(timeoutSec, TimeUnit.SECONDS, TimeSource.NANO_TIME);
        int newVersion = lastVersion;
        synchronized (this) {
            while (!timeout.hasTimedOut() && (newVersion = version) != lastVersion) {
                this.wait(timeout.timeLeft(TimeUnit.MILLISECONDS));
            }
        }
        if (newVersion < lastVersion) {
            // ArithmeticException? This could mean we overflowed
            throw new RuntimeException("Invalid version - went back! currentVersion=" + lastVersion +
                    " newVersion=" + newVersion);
        } else if (newVersion == lastVersion) {
            throw new TimeoutException("Timed out waiting for version change. currentVersion=" + lastVersion +
                    " newVersion=" + newVersion);
        }

        return newVersion;
    }
}

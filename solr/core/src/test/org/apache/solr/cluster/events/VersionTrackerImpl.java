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

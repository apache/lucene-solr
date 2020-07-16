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
package org.apache.solr.common.util;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrQueuedThreadPool extends QueuedThreadPool implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final boolean killStop;
    private final String name;
    private volatile Error error;
    private final Object notify = new Object();



    public SolrQueuedThreadPool(String name, boolean killStop) {
        super(10000, 15,
        15000, -1,
        null, null,
              new  SolrNamedThreadFactory(name));
        this.killStop = killStop;
        this.name = name;
    }

    protected void runJob(Runnable job) {
        try {
            job.run();
        } catch (Error error) {
            log.error("Error in Jetty thread pool thread", error);
            this.error = error;
        }
        synchronized (notify) {
            notify.notifyAll();
        }
    }


//
//    @Override
//    public Thread newThread(Runnable runnable) {
//        Thread thread = new Thread(tg, runnable);
//        thread.setDaemon(isDaemon());
//        thread.setPriority(getThreadsPriority());
//        thread.setName(name + "-" + thread.getId());
//        return thread;d
//    }

    public void close() {
        TimeOut timeout = new TimeOut(15, TimeUnit.SECONDS, TimeSource.NANO_TIME);
        while (getBusyThreads() != 0) {
            if (timeout.hasTimedOut()) {
                throw new RuntimeException("Timed out waiting for SolrQueuedThreadPool to close");
            }
            try {
                synchronized (notify) {
                    notify.wait(500);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (error != null) {
            throw error;
        }
        assert ObjectReleaseTracker.release(this);
    }

    @Override
    protected void doStop() throws Exception {
        if (!killStop) {
           super.doStop();
        }
    }

    public void stdStop() throws Exception {
        super.doStop();
    }

    @Override
    public void join() throws InterruptedException
    {
        if (!killStop) {
            super.join();
        }
    }
}
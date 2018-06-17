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
package org.apache.solr.client.solrj.util;

import java.io.Closeable;
import java.util.concurrent.BlockingQueue;

import org.apache.solr.common.util.ObjectReleaseTracker;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

public class SolrQueuedThreadPool extends QueuedThreadPool implements Closeable {
  public SolrQueuedThreadPool() {
    assert ObjectReleaseTracker.track(this);
  }

  public SolrQueuedThreadPool(int maxThreads, int minThreads, int idleTimeouut, BlockingQueue<Runnable> queue, ThreadGroup threadGroup) {
    super(maxThreads, minThreads, idleTimeouut, queue, threadGroup);
    assert ObjectReleaseTracker.track(this);
  }
  
  @Override
  protected Thread newThread(Runnable runnable) {
    System.out.println("new thread");
    Thread thread = super.newThread(runnable);
    System.out.println(" thread:" + thread.getName() + " " + thread.getId());
    return thread;
  }

  public void close() {
    while (!isStopped()) {
      try {
        // this allows 15 seconds until we start interrupting
        setStopTimeout(60000); // nocommit
        stop();
        
        // now we wait up 30 seconds gracefully, then interrupt again before waiting for the rest of the timeout
       // setStopTimeout(60000);
       // doStop();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    assert ObjectReleaseTracker.release(this);
  }
}

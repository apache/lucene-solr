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
package org.apache.solr.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ParWorkExecutor extends ThreadPoolExecutor {
  private static final Logger log = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  public static final int KEEP_ALIVE_TIME = 30000;

  private static AtomicInteger threadNumber = new AtomicInteger(0);
  private volatile boolean closed;
  private volatile boolean closeLock;

  public ParWorkExecutor(String name, int maxPoolsSize) {
    this(name, 0, maxPoolsSize, KEEP_ALIVE_TIME, new SynchronousQueue<>());
  }

  public ParWorkExecutor(String name, int corePoolsSize, int maxPoolsSize) {
    this(name, corePoolsSize, maxPoolsSize, KEEP_ALIVE_TIME,  new SynchronousQueue<>());
  }

  public ParWorkExecutor(String name, int corePoolsSize, int maxPoolsSize,
      int keepalive, BlockingQueue<Runnable> workQueue) {
    super(corePoolsSize, maxPoolsSize, keepalive, TimeUnit.MILLISECONDS, workQueue
    , new ParWorkThreadFactory(name));
  }

  public void shutdown() {
    if (closeLock) {
      throw new IllegalCallerException();
    }
    this.closed = true;
    super.shutdown();
  }

  public List<Runnable> shutdownNow() {
    if (closeLock) {
      throw new IllegalCallerException();
    }
    this.closed = true;
    super.shutdownNow();
    return Collections.emptyList();
  }

  public void closeLock(boolean lock) {
    this.closeLock = lock;
  }

  private static class ParWorkThreadFactory implements ThreadFactory {

    private final String name;

    public ParWorkThreadFactory(String name) {
      this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
      ThreadGroup group;

      SecurityManager s = System.getSecurityManager();
      group = (s != null)? s.getThreadGroup() :
          Thread.currentThread().getThreadGroup();

      Thread t = new Thread(group,
          name + threadNumber.getAndIncrement()) {
        public void run() {
          try {
            r.run();
          } finally {
            ParWork.closeExecutor();
          }
        }
      };
      t.setDaemon(true);
      return t;
    }
  }
}

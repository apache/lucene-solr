package org.apache.lucene.server;

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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// From Brian Goetz, JCiP:
class BlockingThreadPoolExecutor extends ThreadPoolExecutor {
        
  private final Semaphore semaphore;

  public BlockingThreadPoolExecutor(int bound, int corePoolSize,  int maxPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> queue, ThreadFactory threadFactory) {
    super(corePoolSize, maxPoolSize, keepAliveTime, unit, queue, threadFactory);
    this.semaphore = new Semaphore(bound);
  }

  @Override
  public void execute(Runnable task) {
    try {
      semaphore.acquire();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }

    try {
      super.execute(task);
    } catch(RuntimeException e) {
      // specifically, handle RejectedExecutionException  
      semaphore.release();
      throw e;
    } catch(Error e) {
      semaphore.release();
      throw e;
    }
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    semaphore.release();
  }
}

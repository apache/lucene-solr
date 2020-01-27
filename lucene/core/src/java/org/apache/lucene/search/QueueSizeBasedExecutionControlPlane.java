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

package org.apache.lucene.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Implementation of SliceExecutionControlPlane with queue backpressure based thread allocation
 */
public class QueueSizeBasedExecutionControlPlane<C> implements SliceExecutionControlPlane<C> {
  private static final double LIMITING_FACTOR = 1.5;
  private static final int NUMBER_OF_PROCESSORS = Runtime.getRuntime().availableProcessors();

  private Executor executor;

  public QueueSizeBasedExecutionControlPlane(Executor executor) {
    this.executor = executor;
  }

  @Override
  public List<Future<C>> invokeAll(Collection<FutureTask> tasks) {
    boolean isThresholdCheckEnabled = true;

    if (tasks == null) {
      throw new IllegalArgumentException("Tasks is null");
    }

    if (executor == null) {
      throw new IllegalArgumentException("Executor is null");
    }

    ThreadPoolExecutor threadPoolExecutor = null;
    if ((executor instanceof ThreadPoolExecutor) == false) {
      // No effect
      isThresholdCheckEnabled = false;
    } else {
      threadPoolExecutor = (ThreadPoolExecutor) executor;
    }

    List<Future<C>> futures = new ArrayList();

    for (FutureTask task : tasks) {
      boolean shouldExecuteOnCallerThread = false;

      if (isThresholdCheckEnabled && threadPoolExecutor.getQueue().size() <
          (NUMBER_OF_PROCESSORS * LIMITING_FACTOR)) {
        shouldExecuteOnCallerThread = true;
      }

      try {
        executor.execute(task);
      } catch (RejectedExecutionException e) {
        // Execute on caller thread
        shouldExecuteOnCallerThread = true;
      }

      if (shouldExecuteOnCallerThread) {
        task.run();

      }
      futures.add(task);
    }

    return futures;
  }
}

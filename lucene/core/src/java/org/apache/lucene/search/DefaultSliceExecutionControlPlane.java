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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;

/**
 * Implementation of SliceExecutionControlPlane with queue backpressure based thread allocation
 */
public class DefaultSliceExecutionControlPlane implements SliceExecutionControlPlane<List<Future>, FutureTask> {
  private final Executor executor;

  public DefaultSliceExecutionControlPlane(Executor executor) {
    this.executor = executor;
  }

  @Override
  public List<Future> invokeAll(Collection<FutureTask> tasks) {

    if (tasks == null) {
      throw new IllegalArgumentException("Tasks is null");
    }

    if (executor == null) {
      throw new IllegalArgumentException("Executor is null");
    }

    List<Future> futures = new ArrayList();

    int i = 0;

    for (FutureTask task : tasks) {
      boolean shouldExecuteOnCallerThread = false;

      // Execute last task on caller thread
      if (i == tasks.size() - 1) {
        shouldExecuteOnCallerThread = true;
      }

      processTask(task, futures, shouldExecuteOnCallerThread);
      ++i;
    }

    return futures;
  }

  // Helper method to execute a single task
  protected void processTask(FutureTask task, List<Future> futures,
                             boolean shouldExecuteOnCallerThread) {
    if (task == null) {
      throw new IllegalArgumentException("Input is null");
    }

    if (!shouldExecuteOnCallerThread) {
      try {
        executor.execute(task);
      } catch (RejectedExecutionException e) {
        // Execute on caller thread
        shouldExecuteOnCallerThread = true;
      }
    }

    if (shouldExecuteOnCallerThread) {
      try {
        task.run();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    if (!shouldExecuteOnCallerThread) {
      futures.add(task);
    } else {
      try {
        futures.add(CompletableFuture.completedFuture(task.get()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}

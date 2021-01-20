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

import java.util.Collection;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Derivative of SliceExecutor that controls the number of active threads that are used for a single
 * query. At any point, no more than (maximum pool size of the executor * LIMITING_FACTOR) tasks
 * should be active. If the limit is exceeded, further segments are searched on the caller thread
 */
class QueueSizeBasedExecutor extends SliceExecutor {
  private static final double LIMITING_FACTOR = 1.5;

  private final ThreadPoolExecutor threadPoolExecutor;

  public QueueSizeBasedExecutor(ThreadPoolExecutor threadPoolExecutor) {
    super(threadPoolExecutor);
    this.threadPoolExecutor = threadPoolExecutor;
  }

  @Override
  public void invokeAll(Collection<? extends Runnable> tasks) {
    int i = 0;

    for (Runnable task : tasks) {
      boolean shouldExecuteOnCallerThread = false;

      // Execute last task on caller thread
      if (i == tasks.size() - 1) {
        shouldExecuteOnCallerThread = true;
      }

      if (threadPoolExecutor.getQueue().size()
          >= (threadPoolExecutor.getMaximumPoolSize() * LIMITING_FACTOR)) {
        shouldExecuteOnCallerThread = true;
      }

      processTask(task, shouldExecuteOnCallerThread);

      ++i;
    }
  }
}

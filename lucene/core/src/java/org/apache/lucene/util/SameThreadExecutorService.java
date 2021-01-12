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
package org.apache.lucene.util;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * An {@code ExecutorService} that executes tasks immediately in the calling thread during submit.
 *
 * @lucene.internal
 */
public final class SameThreadExecutorService extends AbstractExecutorService {
  private volatile boolean shutdown;

  @Override
  public void execute(Runnable command) {
    checkShutdown();
    command.run();
  }

  @Override
  public List<Runnable> shutdownNow() {
    shutdown();
    return Collections.emptyList();
  }

  @Override
  public void shutdown() {
    this.shutdown = true;
  }

  @Override
  public boolean isTerminated() {
    // Simplified: we don't check for any threads hanging in execute (we could
    // introduce an atomic counter, but there seems to be no point).
    return shutdown == true;
  }

  @Override
  public boolean isShutdown() {
    return shutdown == true;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    // See comment in isTerminated();
    return true;
  }

  private void checkShutdown() {
    if (shutdown) {
      throw new RejectedExecutionException("Executor is shut down.");
    }
  }
}

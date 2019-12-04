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

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Blocks allocation based on size of waiting queue for passed in ExecutorService
 */
public class QueueSizeBasedCircuitBreaker implements SliceExecutionControlPlane {
  private final Executor executor;
  private static final int NUMBER_OF_PROCESSORS = Runtime.getRuntime().availableProcessors();
  private static final double LIMITING_FACTOR = 1.5;

  public QueueSizeBasedCircuitBreaker(Executor executor) {
    this.executor = executor;
  }

  @Override
  public boolean hasCircuitBreakerTriggered() {
    if ((executor instanceof ThreadPoolExecutor) == false) {
      // No effect
      return true;
    }

    ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;

    return threadPoolExecutor.getQueue().size() < (NUMBER_OF_PROCESSORS * LIMITING_FACTOR);
  }

  @Override
  public void execute(Runnable command) {
    if (command == null) {
      throw new IllegalArgumentException("Null input passed in");
    }

    executor.execute(command);
  }

  @Override
  public Executor getExecutor() {
    return executor;
  }
}

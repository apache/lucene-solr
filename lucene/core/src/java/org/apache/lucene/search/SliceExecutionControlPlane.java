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

/**
 * Manages the end to end execution and management of slices for an IndexSearcher
 * instance. This includes execution on the underlying Executor and managing
 * thread allocations to ensure a consistent throughput under varying stress loads.
 * This class can block allocation of new slices when executing a query
 * Remaining segments will be allocated to a single slice
 * NOTE: This can be degrading to query performance since one thread can
 * be overloaded with multiple segments hence this is a tradeoff
 * between query throughput and latency
 *
 * A typical implementation of this interface would include the execution framework
 * along with a circuit breaker condition which will control whether new threads will be
 * created or not.
 */
public interface SliceExecutionControlPlane {

  /**
   * Return true if the circuit breaker condition has triggered,
   * false otherwise
   */
  boolean hasCircuitBreakerTriggered();

  /**
   * Executes a given task using the underlying Executor
   */
  void execute(Runnable command);

  /**
   * Returns the underlying executor
   */
  Executor getExecutor();

}

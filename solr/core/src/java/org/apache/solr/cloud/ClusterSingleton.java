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
package org.apache.solr.cloud;

/**
 * Intended for {@link org.apache.solr.core.CoreContainer} plugins that should be
 * enabled only one instance per cluster.
 * <p>Components that implement this interface are always in one of two states:
 * <ul>
 *   <li>STOPPED - the default state. The component is idle and does not perform
 *   any functions. It should also avoid holding any resources.</li>
 *   <li>RUNNING - the component is active.</li>
 * </ul>
 * <p>Components must be prepared to change these states multiple times in their
 * life-cycle.</p>
 * <p>Implementation detail: currently these plugins are instantiated on all nodes
 * but they are started only on the Overseer leader, and stopped when the current
 * node loses its leadership.</p>
 */
public interface ClusterSingleton {

  /**
   * Unique name of this singleton. Used for registration.
   */
  String getName();

  /**
   * Start the operation of the component. On return the component is assumed
   * to be in the RUNNING state.
   * @throws Exception on startup errors. The component should revert to the
   * STOPPED state.
   */
  void start() throws Exception;

  /**
   * Returns true if the component is in the RUNNING state, false otherwise.
   */
  boolean isRunning();

  /**
   * Stop the operation of the component. On return the component is assumed
   * to be in the STOPPED state. Components should also avoid holding any resources
   * in the STOPPED state.
   */
  void stop();
}

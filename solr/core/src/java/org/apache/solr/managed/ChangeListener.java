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
package org.apache.solr.managed;

/**
 * Listen to changes in resource limit settings caused by resource management framework
 * (or by users via resource management API).
 * <p>Note: implementations must provide unique and stable {@link Object#equals(Object)} / {@link Object#hashCode()} methods
 * to avoid duplicates when registering.</p>
 */
public interface ChangeListener {

  enum Reason {
    /** Administrative user action. */
    USER,
    /** Adjustment made to optimize the resource behavior. */
    OPTIMIZATION,
    /** Adjustment made due to total limit exceeded. */
    ABOVE_TOTAL_LIMIT,
    /** Adjustment made due to total limit underuse. */
    BELOW_TOTAL_LIMIT,
    /** Adjustment made due to individual resource limit exceeded. */
    ABOVE_LIMIT,
    /** Adjustment made due to individual resource limit underuse. */
    BELOW_LIMIT,
    /** Other unspecified reason. */
    OTHER
  }

  /**
   * Notify about changing a limit of a resource.
   * @param poolName pool name where resource is managed.
   * @param component managed component
   * @param limitName limit name
   * @param newRequestedVal requested new value of the resource limit.
   * @param newActualVal actual value applied to the resource configuration. Note: this may differ from the
   *                     value requested due to internal logic of the component.
   * @param reason reason of the change
   */
  void changedLimit(String poolName, ManagedComponent component, String limitName, Object newRequestedVal, Object newActualVal, Reason reason);

  /**
   * @param poolName pool name where resource is managed.
   * @param component managed component
   * @param limitName limit name
   * @param newRequestedVal requested new value of the resource limit.
   * @param reason reason of the change
   * @param error error encountered while changing. When an error occurs the actual current value of the
   *              limit is unknown and must be verified.
   */
  default void onError(String poolName, ManagedComponent component, String limitName, Object newRequestedVal, Reason reason, Throwable error) {

  }
}

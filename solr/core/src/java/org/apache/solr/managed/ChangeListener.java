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
 *
 */
public interface ChangeListener {

  /**
   * Notify about changing a limit of a resource.
   * @param poolName pool name where resource is managed.
   * @param component managed component
   * @param limitName limit name
   * @param newRequestedVal requested new value of the resource limit.
   * @param newActualVal actual value applied to the resource configuration. Note: this may differ from the
   *                     value requested due to internal logic of the component.
   */
  void changedLimit(String poolName, ManagedComponent component, String limitName, Object newRequestedVal, Object newActualVal);
}

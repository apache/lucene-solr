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
package org.apache.solr.core;

import java.util.List;

/**
 * Manage the discovery and persistence of core definitions across Solr restarts
 */
public interface CoresLocator {

  /**
   * Make new cores available for discovery
   * @param cc              the CoreContainer
   * @param coreDescriptors CoreDescriptors to persist
   */
  public void create(CoreContainer cc, CoreDescriptor... coreDescriptors);

  /**
   * Ensure that the core definitions from the passed in CoreDescriptors
   * will persist across container restarts.
   * @param cc              the CoreContainer
   * @param coreDescriptors CoreDescriptors to persist
   */
  public void persist(CoreContainer cc, CoreDescriptor... coreDescriptors);

  /**
   * Ensure that the core definitions from the passed in CoreDescriptors
   * are not available for discovery
   * @param cc              the CoreContainer
   * @param coreDescriptors CoreDescriptors of the cores to remove
   */
  public void delete(CoreContainer cc, CoreDescriptor... coreDescriptors);

  /**
   * Persist the new name of a renamed core
   * @param cc    the CoreContainer
   * @param oldCD the CoreDescriptor of the core before renaming
   * @param newCD the CoreDescriptor of the core after renaming
   */
  public void rename(CoreContainer cc, CoreDescriptor oldCD, CoreDescriptor newCD);

  /**
   * Swap two core definitions
   * @param cc  the CoreContainer
   * @param cd1 the core descriptor of the first core, after swapping
   * @param cd2 the core descriptor of the second core, after swapping
   */
  public void swap(CoreContainer cc, CoreDescriptor cd1, CoreDescriptor cd2);

  /**
   * Load all the CoreDescriptors from persistence store
   * @param cc the CoreContainer
   * @return a list of all CoreDescriptors found
   */
  public List<CoreDescriptor> discover(CoreContainer cc);
}

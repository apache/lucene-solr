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
 * A managed component.
 */
public interface ManagedComponent extends AutoCloseable {
  /**
   * Unique name of this component. By convention id-s form a colon-separated hierarchy.
   */
  ManagedComponentId getManagedComponentId();

  void initializeManagedComponent(ResourceManager resourceManager, String poolName, String... otherPools);

  /**
   * Component context for managing additional state related to the resource management.
   */
  SolrResourceContext getSolrResourceContext();

  /**
   * Close and unregister this component from any resource pools. IMPORTANT: implementations must
   * call this method or otherwise close the component's resource context.
   * @throws Exception on context close errors
   */
  default void close() throws Exception {
    if (getSolrResourceContext() != null) {
      getSolrResourceContext().close();
    }
  }
}

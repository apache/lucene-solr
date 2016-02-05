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
package org.apache.solr.rest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;

/**
 * Allows a Solr component to register as an observer of important
 * ManagedResource events, such as when the managed data is loaded.
 */
public interface ManagedResourceObserver {
  /**
   * Event notification raised once during core initialization to notify
   * listeners that a ManagedResource is fully initialized. The most 
   * common implementation of this method is to pull the managed data from
   * the concrete ManagedResource and use it to initialize an analysis component.
   * For example, the ManagedStopFilterFactory implements this method to
   * receive the list of managed stop words needed to create a CharArraySet 
   * for the StopFilter. 
   */
  void onManagedResourceInitialized(NamedList<?> args, ManagedResource res)
      throws SolrException;
}

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

package org.apache.solr.cloud.autoscaling;

import java.io.Closeable;
import java.util.Map;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.core.SolrResourceLoader;

/**
 * Interface for actions performed in response to a trigger being activated
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public interface TriggerAction extends Closeable {

  /**
   * Called when action is created but before it's initialized and used.
   * This method should also verify that the configuration parameters are correct.
   * It may be called multiple times.
   * @param loader loader to use for instantiating sub-components
   * @param cloudManager current instance of SolrCloudManager
   * @param properties configuration properties
   * @throws TriggerValidationException contains details of invalid configuration parameters.
   */
  void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> properties) throws TriggerValidationException;

  /**
   * Called before an action is first used. Any heavy object creation or initialization should
   * be done in this method instead of the constructor or {@link #configure(SolrResourceLoader, SolrCloudManager, Map)} method.
   */
  void init() throws Exception;

  String getName();

  void process(TriggerEvent event, ActionContext context) throws Exception;
}

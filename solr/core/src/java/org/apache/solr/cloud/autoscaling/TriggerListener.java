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

import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.core.SolrResourceLoader;

/**
 * Implementations of this interface are notified of stages in event processing that they were
 * registered for. Note: instances may be closed and re-created on each auto-scaling config update.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public interface TriggerListener extends Closeable {

  /**
   * Called when listener is created but before it's initialized and used.
   * This method should also verify that the configuration parameters are correct.
   * It may be called multiple times.
   * @param loader loader to use for instantiating sub-components
   * @param cloudManager current instance of SolrCloudManager
   * @param config coniguration
   * @throws TriggerValidationException contains details of invalid configuration parameters.
   */
  void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, AutoScalingConfig.TriggerListenerConfig config) throws TriggerValidationException;

  /**
   * If this method returns false then the listener's {@link #onEvent(TriggerEvent, TriggerEventProcessorStage, String, ActionContext, Throwable, String)}
   * method should not be called.
   */
  boolean isEnabled();

  void init() throws Exception;

  AutoScalingConfig.TriggerListenerConfig getConfig();

  /**
   * This method is called when either a particular <code>stage</code> or
   * <code>actionName</code> is reached during event processing.
   * @param event current event being processed
   * @param stage {@link TriggerEventProcessorStage} that this listener was registered for, or null
   * @param actionName {@link TriggerAction} name that this listener was registered for, or null
   * @param context optional {@link ActionContext} when the processing stage is related to an action, or null
   * @param error optional {@link Throwable} error, or null
   * @param message optional message
   */
  void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName, ActionContext context,
               Throwable error, String message) throws Exception;
}

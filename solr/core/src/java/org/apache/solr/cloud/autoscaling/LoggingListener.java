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

import java.lang.invoke.MethodHandles;

import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link TriggerListener} that reports
 * events to a log.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class LoggingListener extends TriggerListenerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName, ActionContext context,
                      Throwable error, String message) {
    log.info("{}: stage={}, actionName={}, event={}, error={}, messsage={}", config.name, stage, actionName, event, error, message);
  }
}

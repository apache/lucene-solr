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
package org.apache.solr.client.solrj.cloud.autoscaling;

import java.lang.invoke.MethodHandles;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.params.CollectionParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This suggester simply logs the request but does not produce any suggestions.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class UnsupportedSuggester extends Suggester {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CollectionParams.CollectionAction action;

  public static UnsupportedSuggester get(Policy.Session session, CollectionParams.CollectionAction action) {
    UnsupportedSuggester suggester = new UnsupportedSuggester(action);
    suggester._init(session);
    return suggester;
  }

  public UnsupportedSuggester(CollectionParams.CollectionAction action) {
    this.action = action;
  }

  @Override
  public CollectionParams.CollectionAction getAction() {
    return action;
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  SolrRequest init() {
    log.warn("Unsupported suggester for action {} with hings {} - no suggestion available", action, hints);
    return null;
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public SolrRequest getSuggestion() {
    return null;
  }
}

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
package org.apache.solr.handler.clustering;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.SchemaField;

import java.util.function.Supplier;

/**
 * Parses each clustering engine configuration
 * initialization parameters.
 */
final class EngineEntry implements Supplier<Engine> {
  /**
   * Marks the engine as optional (if unavailable).
   */
  private static final String PARAM_OPTIONAL = "optional";

  /**
   * Unique engine name parameter.
   */
  private static final String PARAM_NAME = "name";

  final boolean optional;
  final String engineName;
  final EngineParameters defaults;

  /**
   * Preinitialized instance of a clustering engine.
   */
  private Engine engine;

  /**
   * {@code true} if the engine has been initialized properly and is available.
   */
  private boolean available;

  EngineEntry(SolrParams params) {
    this.optional = params.getBool(PARAM_OPTIONAL, false);
    this.engineName = params.get(PARAM_NAME, "");

    defaults = new EngineParameters(params);
  }

  boolean initialize(SolrCore core) {
    SchemaField uniqueField = core.getLatestSchema().getUniqueKeyField();
    if (uniqueField == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          ClusteringComponent.class.getSimpleName() + " requires the declaration of uniqueKeyField in the schema.");
    }
    String docIdField = uniqueField.getName();
    defaults.setDocIdField(docIdField);

    engine = new Engine();
    available = engine.init(engineName, core, defaults);
    return available;
  }

  @Override
  public Engine get() {
    return engine;
  }
}

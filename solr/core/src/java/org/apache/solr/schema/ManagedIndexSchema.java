package org.apache.solr.schema;
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

import org.apache.solr.core.SolrConfig;
import org.xml.sax.InputSource;

/** Solr-managed schema - non-user-editable, but can be mutable via internal and external REST API requests. */
public final class ManagedIndexSchema extends IndexSchema {

  private boolean isMutable = false;

  @Override
  public boolean isMutable() {
    return isMutable;
  }
  
  /**
   * Constructs a schema using the specified resource name and stream.
   *
   * @see org.apache.solr.core.SolrResourceLoader#openSchema
   *      By default, this follows the normal config path directory searching rules.
   * @see org.apache.solr.core.SolrResourceLoader#openResource
   */
  ManagedIndexSchema(SolrConfig solrConfig, String name, InputSource is, boolean isMutable) {
    super(solrConfig, name, is);
    this.isMutable = isMutable;
  }
}

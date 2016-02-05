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
package org.apache.solr.client.solrj;

import org.apache.solr.common.SolrDocument;

/**
 * A callback interface for streaming response
 * 
 * @since solr 4.0
 */
public abstract class StreamingResponseCallback {
  /*
   * Called for each SolrDocument in the response
   */
  public abstract void streamSolrDocument( SolrDocument doc );

  /*
   * Called at the beginning of each DocList (and SolrDocumentList)
   */
  public abstract void streamDocListInfo( long numFound, long start, Float maxScore );
}

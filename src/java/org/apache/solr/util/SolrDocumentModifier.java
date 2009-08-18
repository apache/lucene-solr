package org.apache.solr.util;
/**
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

import org.apache.solr.common.SolrDocument;


/**
 * Callback capability for modifying a SolrDocument in the {@link SolrPluginUtils#docListToSolrDocumentList(org.apache.solr.search.DocList, org.apache.solr.search.SolrIndexSearcher, java.util.Set, java.util.Map)}
 *
 * <p/>
 * NOTE: This API is subject to change.
 * Due to https://issues.apache.org/jira/browse/SOLR-1298 and https://issues.apache.org/jira/browse/SOLR-705, this interface may change in the future.
 *
 **/
public interface SolrDocumentModifier {
  /**
   * Implement this method to allow for changes to be made to the {@link org.apache.solr.common.SolrDocument} in the {@link SolrPluginUtils#docListToSolrDocumentList(org.apache.solr.search.DocList, org.apache.solr.search.SolrIndexSearcher, java.util.Set, SolrDocumentModifier, java.util.Map)}
   * call.
   * @param doc The {@link org.apache.solr.common.SolrDocument} that can be modified.
   */
  void process(SolrDocument doc);
}

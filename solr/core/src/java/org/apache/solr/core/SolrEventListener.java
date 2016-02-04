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
package org.apache.solr.core;

import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 *
 */
public interface SolrEventListener extends NamedListInitializedPlugin{

  public void postCommit();
  
  public void postSoftCommit();

  /** The searchers passed here are only guaranteed to be valid for the duration
   * of this method call, so care should be taken not to spawn threads or asynchronous
   * tasks with references to these searchers.
   * <p>
   * Implementations should add the {@link org.apache.solr.common.params.EventParams#EVENT} parameter and set it to a value of either:
   * <ul>
   * <li>{@link org.apache.solr.common.params.EventParams#FIRST_SEARCHER} - First Searcher event</li>
   * <li>{@link org.apache.solr.common.params.EventParams#NEW_SEARCHER} - New Searcher event</li>
   * </ul>
   *
   * Sample:
   * <pre>
    if (currentSearcher != null) {
      nlst.add(CommonParams.EVENT, CommonParams.NEW_SEARCHER);
    } else {
      nlst.add(CommonParams.EVENT, CommonParams.FIRST_SEARCHER);
    }
   *
   * </pre>
   *
   * @see org.apache.solr.core.AbstractSolrEventListener#addEventParms(org.apache.solr.search.SolrIndexSearcher, org.apache.solr.common.util.NamedList) 
   *
   * @param newSearcher The new {@link org.apache.solr.search.SolrIndexSearcher} to use
   * @param currentSearcher The existing {@link org.apache.solr.search.SolrIndexSearcher}.  null if this is a firstSearcher event.
   *
   */
  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher);

}

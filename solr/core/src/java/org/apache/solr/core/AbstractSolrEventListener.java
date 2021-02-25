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

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.params.EventParams;
import org.apache.solr.search.SolrIndexSearcher;

/**
 */
public class AbstractSolrEventListener implements SolrEventListener {
  private final SolrCore core;
  public SolrCore getCore() { return core; }

  public AbstractSolrEventListener(SolrCore core) {
    this.core = core;
  }
  @SuppressWarnings({"rawtypes"})
  private NamedList args;
  @SuppressWarnings({"rawtypes"})
  public NamedList getArgs() { return args; }

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    this.args = args.clone();
  }

  @Override
  public void postCommit() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void postSoftCommit() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return getClass().getName() + args;
  }

  /**
   * Add the {@link org.apache.solr.common.params.EventParams#EVENT} with either the {@link org.apache.solr.common.params.EventParams#NEW_SEARCHER}
   * or {@link org.apache.solr.common.params.EventParams#FIRST_SEARCHER} values depending on the value of currentSearcher.
   * <p>
   * Makes a copy of NamedList and then adds the parameters.
   *
   *
   * @param currentSearcher If null, add FIRST_SEARCHER, otherwise NEW_SEARCHER
   * @param nlst The named list to add the EVENT value to
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  protected NamedList addEventParms(SolrIndexSearcher currentSearcher, NamedList nlst) {
    NamedList result = new NamedList();
    result.addAll(nlst);
    if (currentSearcher != null) {
      result.add(EventParams.EVENT, EventParams.NEW_SEARCHER);
    } else {
      result.add(EventParams.EVENT, EventParams.FIRST_SEARCHER);
    }
    return result;
  }
}

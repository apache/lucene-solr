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
import org.apache.solr.pkg.PackagePluginHolder;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * A {@link SolrEventListener} wrapper that loads class from  a package
 * and reload if it's modified
 */
public class DelegatingEventListener implements SolrEventListener {

  private final PackagePluginHolder<SolrEventListener> holder;

  public DelegatingEventListener(PackagePluginHolder<SolrEventListener> holder) {
    this.holder = holder;
  }


  @Override
  public void postCommit() {
   holder.getInstance().ifPresent(SolrEventListener::postCommit);
  }

  @Override
  public void postSoftCommit() {
    holder.getInstance().ifPresent(SolrEventListener::postSoftCommit);
  }

  @Override
  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    holder.getInstance().ifPresent(it -> it.newSearcher(newSearcher, currentSearcher));
  }

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {

  }
}

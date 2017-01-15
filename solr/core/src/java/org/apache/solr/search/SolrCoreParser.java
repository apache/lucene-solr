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
package org.apache.solr.search;

import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.xml.CoreParser;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * Assembles a QueryBuilder which uses Query objects from Solr's <code>search</code> module
 * in addition to Query objects supported by the Lucene <code>CoreParser</code>.
 */
public class SolrCoreParser extends CoreParser implements NamedListInitializedPlugin {

  protected final SolrQueryRequest req;

  public SolrCoreParser(String defaultField, Analyzer analyzer,
      SolrQueryRequest req) {
    super(defaultField, analyzer);
    queryFactory.addBuilder("LegacyNumericRangeQuery", new LegacyNumericRangeQueryBuilder());
    this.req = req;
  }

  @Override
  public void init(NamedList initArgs) {
    if (initArgs == null || initArgs.size() == 0) {
      return;
    }
    final SolrResourceLoader loader;
    if (req == null) {
      loader = new SolrResourceLoader();
    } else {
      loader = req.getCore().getResourceLoader();
    }

    final Iterable<Map.Entry<String,Object>> args = initArgs;
    for (final Map.Entry<String,Object> entry : args) {
      final String queryName = entry.getKey();
      final String queryBuilderClassName = (String)entry.getValue();

      final SolrQueryBuilder queryBuilder = loader.newInstance(
          queryBuilderClassName,
          SolrQueryBuilder.class,
          null,
          new Class[] {String.class, Analyzer.class, SolrQueryRequest.class, QueryBuilder.class},
          new Object[] {defaultField, analyzer, req, this});

      this.queryFactory.addBuilder(queryName, queryBuilder);
    }
  }

}

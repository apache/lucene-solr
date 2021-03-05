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

import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.xml.CoreParser;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.queryparser.xml.builders.SpanQueryBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.XMLErrorLogger;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ErrorHandler;

/**
 * Assembles a QueryBuilder which uses Query objects from Solr's <code>search</code> module
 * in addition to Query objects supported by the Lucene <code>CoreParser</code>.
 */
public class SolrCoreParser extends CoreParser implements NamedListInitializedPlugin {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final XMLErrorLogger xmllog = new XMLErrorLogger(log);

  protected final SolrQueryRequest req;

  public SolrCoreParser(String defaultField, Analyzer analyzer, SolrQueryRequest req) {
    super(defaultField, analyzer);
    queryFactory.addBuilder("LegacyNumericRangeQuery", new LegacyNumericRangeQueryBuilder());
    this.req = req;
    if (null == req) {
      throw new NullPointerException("req must not be null");
    }
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void init(NamedList initArgs) {
    if (initArgs == null || initArgs.size() == 0) {
      return;
    }
    final SolrResourceLoader loader = req.getCore().getResourceLoader();

    final Iterable<Map.Entry<String,Object>> args = initArgs;
    for (final Map.Entry<String,Object> entry : args) {
      final String queryName = entry.getKey();
      final String queryBuilderClassName = (String)entry.getValue();

      try {
        final SolrSpanQueryBuilder spanQueryBuilder = loader.newInstance(
            queryBuilderClassName,
            SolrSpanQueryBuilder.class,
            null,
            new Class[] {String.class, Analyzer.class, SolrQueryRequest.class, SpanQueryBuilder.class},
            new Object[] {defaultField, analyzer, req, this});

        this.addSpanQueryBuilder(queryName, spanQueryBuilder);
      } catch (Exception outerException) {
        try {
        final SolrQueryBuilder queryBuilder = loader.newInstance(
            queryBuilderClassName,
            SolrQueryBuilder.class,
            null,
            new Class[] {String.class, Analyzer.class, SolrQueryRequest.class, QueryBuilder.class},
            new Object[] {defaultField, analyzer, req, this});

        this.addQueryBuilder(queryName, queryBuilder);
        } catch (Exception innerException) {
          log.error("Class {} not found or not suitable: {} {}",
              queryBuilderClassName, outerException, innerException);
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "Cannot find suitable "
                  + SolrSpanQueryBuilder.class.getCanonicalName() + " or "
                  + SolrQueryBuilder.class.getCanonicalName() + " class: "
                  + queryBuilderClassName + " in "
                  + loader);
        }
      }
    }
  }

  @Override
  protected ErrorHandler getErrorHandler() {
    return xmllog;
  }

}

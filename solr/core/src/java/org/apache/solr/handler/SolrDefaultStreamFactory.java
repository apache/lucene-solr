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
package org.apache.solr.handler;

import org.apache.solr.client.solrj.io.Lang;
import org.apache.solr.client.solrj.io.stream.expr.DefaultStreamFactory;
import org.apache.solr.core.SolrResourceLoader;

/**
 * A default collection of mappings, used to convert strings into stream expressions.
 * Same as {@link DefaultStreamFactory} plus functions that rely directly on either
 * Lucene or Solr capabilities that are not part of {@link Lang}.
 *
 * @since 7.5
 */
public class SolrDefaultStreamFactory extends DefaultStreamFactory {

  private SolrResourceLoader solrResourceLoader;

  public SolrDefaultStreamFactory() {
    super();
    this.withFunctionName("analyze",  AnalyzeEvaluator.class);
    this.withFunctionName("cat", CatStream.class);
    this.withFunctionName("classify", ClassifyStream.class);
    this.withFunctionName("haversineMeters", HaversineMetersEvaluator.class);
  }

  public SolrDefaultStreamFactory withSolrResourceLoader(SolrResourceLoader solrResourceLoader) {
    this.solrResourceLoader = solrResourceLoader;
    return this;
  }

  public void setSolrResourceLoader(SolrResourceLoader solrResourceLoader) {
    this.solrResourceLoader = solrResourceLoader;
  }

  public SolrResourceLoader getSolrResourceLoader() {
    return solrResourceLoader;
  }

}

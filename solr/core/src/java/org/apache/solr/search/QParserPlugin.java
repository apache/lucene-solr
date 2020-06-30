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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.join.BlockJoinChildQParserPlugin;
import org.apache.solr.search.join.BlockJoinParentQParserPlugin;
import org.apache.solr.search.join.FiltersQParserPlugin;
import org.apache.solr.search.join.GraphQParserPlugin;
import org.apache.solr.search.join.HashRangeQParserPlugin;
import org.apache.solr.search.mlt.MLTQParserPlugin;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

public abstract class QParserPlugin implements NamedListInitializedPlugin, SolrInfoBean {
  /** internal use - name of the default parser */
  public static final String DEFAULT_QTYPE = LuceneQParserPlugin.NAME;

  /**
   * Internal use - name to parser for the builtin parsers.
   * Each query parser plugin extending {@link QParserPlugin} has own instance of standardPlugins.
   * This leads to cyclic dependencies of static fields and to case when NAME field is not yet initialized.
   * This result to NPE during initialization.
   * For every plugin, listed here, NAME field has to be final and static.
   */
  public static final Map<String, QParserPlugin> standardPlugins;

  static {
    HashMap<String, QParserPlugin> map = new HashMap<>(30, 1);
    map.put(LuceneQParserPlugin.NAME, new LuceneQParserPlugin());
    map.put(FunctionQParserPlugin.NAME, new FunctionQParserPlugin());
    map.put(PrefixQParserPlugin.NAME, new PrefixQParserPlugin());
    map.put(BoostQParserPlugin.NAME, new BoostQParserPlugin());
    map.put(DisMaxQParserPlugin.NAME, new DisMaxQParserPlugin());
    map.put(ExtendedDismaxQParserPlugin.NAME, new ExtendedDismaxQParserPlugin());
    map.put(FieldQParserPlugin.NAME, new FieldQParserPlugin());
    map.put(RawQParserPlugin.NAME, new RawQParserPlugin());
    map.put(TermQParserPlugin.NAME, new TermQParserPlugin());
    map.put(TermsQParserPlugin.NAME, new TermsQParserPlugin());
    map.put(NestedQParserPlugin.NAME, new NestedQParserPlugin());
    map.put(FunctionRangeQParserPlugin.NAME, new FunctionRangeQParserPlugin());
    map.put(SpatialFilterQParserPlugin.NAME, new SpatialFilterQParserPlugin());
    map.put(SpatialBoxQParserPlugin.NAME, new SpatialBoxQParserPlugin());
    map.put(JoinQParserPlugin.NAME, new JoinQParserPlugin());
    map.put(SurroundQParserPlugin.NAME, new SurroundQParserPlugin());
    map.put(SwitchQParserPlugin.NAME, new SwitchQParserPlugin());
    map.put(MaxScoreQParserPlugin.NAME, new MaxScoreQParserPlugin());
    map.put(BlockJoinParentQParserPlugin.NAME, new BlockJoinParentQParserPlugin());
    map.put(BlockJoinChildQParserPlugin.NAME, new BlockJoinChildQParserPlugin());
    map.put(FiltersQParserPlugin.NAME, new FiltersQParserPlugin());
    map.put(CollapsingQParserPlugin.NAME, new CollapsingQParserPlugin());
    map.put(SimpleQParserPlugin.NAME, new SimpleQParserPlugin());
    map.put(ComplexPhraseQParserPlugin.NAME, new ComplexPhraseQParserPlugin());
    map.put(ReRankQParserPlugin.NAME, new ReRankQParserPlugin());
    map.put(ExportQParserPlugin.NAME, new ExportQParserPlugin());
    map.put(MLTQParserPlugin.NAME, new MLTQParserPlugin());
    map.put(HashQParserPlugin.NAME, new HashQParserPlugin());
    map.put(GraphQParserPlugin.NAME, new GraphQParserPlugin());
    map.put(XmlQParserPlugin.NAME, new XmlQParserPlugin());
    map.put(GraphTermsQParserPlugin.NAME, new GraphTermsQParserPlugin());
    map.put(IGainTermsQParserPlugin.NAME, new IGainTermsQParserPlugin());
    map.put(TextLogisticRegressionQParserPlugin.NAME, new TextLogisticRegressionQParserPlugin());
    map.put(SignificantTermsQParserPlugin.NAME, new SignificantTermsQParserPlugin());
    map.put(PayloadScoreQParserPlugin.NAME, new PayloadScoreQParserPlugin());
    map.put(PayloadCheckQParserPlugin.NAME, new PayloadCheckQParserPlugin());
    map.put(BoolQParserPlugin.NAME, new BoolQParserPlugin());
    map.put(MinHashQParserPlugin.NAME, new MinHashQParserPlugin());
    map.put(HashRangeQParserPlugin.NAME, new HashRangeQParserPlugin());
    map.put(RankQParserPlugin.NAME, new RankQParserPlugin());

    standardPlugins = Collections.unmodifiableMap(map);
  }

  /** return a {@link QParser} */
  public abstract QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req);

  @Override
  public void init( @SuppressWarnings({"rawtypes"})NamedList args ) {
  }

  @Override
  public String getName() {
    // TODO: ideally use the NAME property that each qparser plugin has

    return this.getClass().getName();
  }

  @Override
  public String getDescription() {
    return "";  // UI required non-null to work
  }

  @Override
  public Category getCategory() {
    return Category.QUERYPARSER;
  }

  @Override
  public Set<String> getMetricNames() {
    return null;
  }

}



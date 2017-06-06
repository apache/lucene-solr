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
import org.apache.solr.search.join.GraphQParserPlugin;
import org.apache.solr.search.mlt.MLTQParserPlugin;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

public abstract class QParserPlugin implements NamedListInitializedPlugin, SolrInfoBean {
  /** internal use - name of the default parser */
  public static final String DEFAULT_QTYPE = LuceneQParserPlugin.NAME;

  /**
   * Internal use - name to class mappings of builtin parsers.
   * Each query parser plugin extending {@link QParserPlugin} has own instance of standardPlugins.
   * This leads to cyclic dependencies of static fields and to case when NAME field is not yet initialized.
   * This result to NPE during initialization.
   * For every plugin, listed here, NAME field has to be final and static.
   */
  public static final Map<String, Class<? extends QParserPlugin>> standardPlugins;

  static {
    HashMap<String, Class<? extends QParserPlugin>> map = new HashMap<>(30, 1);
    map.put(LuceneQParserPlugin.NAME, LuceneQParserPlugin.class);
    map.put(FunctionQParserPlugin.NAME, FunctionQParserPlugin.class);
    map.put(PrefixQParserPlugin.NAME, PrefixQParserPlugin.class);
    map.put(BoostQParserPlugin.NAME, BoostQParserPlugin.class);
    map.put(DisMaxQParserPlugin.NAME, DisMaxQParserPlugin.class);
    map.put(ExtendedDismaxQParserPlugin.NAME, ExtendedDismaxQParserPlugin.class);
    map.put(FieldQParserPlugin.NAME, FieldQParserPlugin.class);
    map.put(RawQParserPlugin.NAME, RawQParserPlugin.class);
    map.put(TermQParserPlugin.NAME, TermQParserPlugin.class);
    map.put(TermsQParserPlugin.NAME, TermsQParserPlugin.class);
    map.put(NestedQParserPlugin.NAME, NestedQParserPlugin.class);
    map.put(FunctionRangeQParserPlugin.NAME, FunctionRangeQParserPlugin.class);
    map.put(SpatialFilterQParserPlugin.NAME, SpatialFilterQParserPlugin.class);
    map.put(SpatialBoxQParserPlugin.NAME, SpatialBoxQParserPlugin.class);
    map.put(JoinQParserPlugin.NAME, JoinQParserPlugin.class);
    map.put(SurroundQParserPlugin.NAME, SurroundQParserPlugin.class);
    map.put(SwitchQParserPlugin.NAME, SwitchQParserPlugin.class);
    map.put(MaxScoreQParserPlugin.NAME, MaxScoreQParserPlugin.class);
    map.put(BlockJoinParentQParserPlugin.NAME, BlockJoinParentQParserPlugin.class);
    map.put(BlockJoinChildQParserPlugin.NAME, BlockJoinChildQParserPlugin.class);
    map.put(CollapsingQParserPlugin.NAME, CollapsingQParserPlugin.class);
    map.put(SimpleQParserPlugin.NAME, SimpleQParserPlugin.class);
    map.put(ComplexPhraseQParserPlugin.NAME, ComplexPhraseQParserPlugin.class);
    map.put(ReRankQParserPlugin.NAME, ReRankQParserPlugin.class);
    map.put(ExportQParserPlugin.NAME, ExportQParserPlugin.class);
    map.put(MLTQParserPlugin.NAME, MLTQParserPlugin.class);
    map.put(HashQParserPlugin.NAME, HashQParserPlugin.class);
    map.put(GraphQParserPlugin.NAME, GraphQParserPlugin.class);
    map.put(XmlQParserPlugin.NAME, XmlQParserPlugin.class);
    map.put(GraphTermsQParserPlugin.NAME, GraphTermsQParserPlugin.class);
    map.put(IGainTermsQParserPlugin.NAME, IGainTermsQParserPlugin.class);
    map.put(TextLogisticRegressionQParserPlugin.NAME, TextLogisticRegressionQParserPlugin.class);
    map.put(SignificantTermsQParserPlugin.NAME, SignificantTermsQParserPlugin.class);
    map.put(PayloadScoreQParserPlugin.NAME, PayloadScoreQParserPlugin.class);
    map.put(PayloadCheckQParserPlugin.NAME, PayloadCheckQParserPlugin.class);

    standardPlugins = Collections.unmodifiableMap(map);
  }

  /** return a {@link QParser} */
  public abstract QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req);

  @Override
  public void init( NamedList args ) {
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



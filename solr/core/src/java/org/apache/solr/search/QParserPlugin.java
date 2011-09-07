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
package org.apache.solr.search;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

public abstract class QParserPlugin implements NamedListInitializedPlugin {
  /** internal use - name of the default parser */
  public static String DEFAULT_QTYPE = LuceneQParserPlugin.NAME;

  /** internal use - name to class mappings of builtin parsers */
  public static final Object[] standardPlugins = {
    LuceneQParserPlugin.NAME, LuceneQParserPlugin.class,
    OldLuceneQParserPlugin.NAME, OldLuceneQParserPlugin.class,
    FunctionQParserPlugin.NAME, FunctionQParserPlugin.class,
    PrefixQParserPlugin.NAME, PrefixQParserPlugin.class,
    BoostQParserPlugin.NAME, BoostQParserPlugin.class,
    DisMaxQParserPlugin.NAME, DisMaxQParserPlugin.class,
    ExtendedDismaxQParserPlugin.NAME, ExtendedDismaxQParserPlugin.class,
    FieldQParserPlugin.NAME, FieldQParserPlugin.class,
    RawQParserPlugin.NAME, RawQParserPlugin.class,
    TermQParserPlugin.NAME, TermQParserPlugin.class,
    NestedQParserPlugin.NAME, NestedQParserPlugin.class,
    FunctionRangeQParserPlugin.NAME, FunctionRangeQParserPlugin.class,
    SpatialFilterQParserPlugin.NAME, SpatialFilterQParserPlugin.class,
    SpatialBoxQParserPlugin.NAME, SpatialBoxQParserPlugin.class,
    JoinQParserPlugin.NAME, JoinQParserPlugin.class,
    SurroundQParserPlugin.NAME, SurroundQParserPlugin.class,
  };

  /** return a {@link QParser} */
  public abstract QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req);
}



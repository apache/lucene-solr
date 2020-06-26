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
package org.apache.solr.response.transform;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * New instance for each request
 *
 *
 */
public abstract class TransformerFactory implements NamedListInitializedPlugin
{
  protected String defaultUserArgs = null;

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    defaultUserArgs = (String)args.get( "args" );
  }

  public abstract DocTransformer create(String field, SolrParams params, SolrQueryRequest req);

  public static final Map<String,TransformerFactory> defaultFactories = new HashMap<>(9, 1.0f);
  static {
    defaultFactories.put( "explain", new ExplainAugmenterFactory() );
    defaultFactories.put( "value", new ValueAugmenterFactory() );
    defaultFactories.put( "docid", new DocIdAugmenterFactory() );
    defaultFactories.put( "shard", new ShardAugmenterFactory() );
    defaultFactories.put( "child", new ChildDocTransformerFactory() );
    defaultFactories.put( "subquery", new SubQueryAugmenterFactory() );
    defaultFactories.put( "json", new RawValueTransformerFactory("json") );
    defaultFactories.put( "xml", new RawValueTransformerFactory("xml") );
    defaultFactories.put( "geo", new GeoTransformerFactory() );
  }
}

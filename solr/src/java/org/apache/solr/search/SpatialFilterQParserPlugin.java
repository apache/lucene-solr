package org.apache.solr.search;
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

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Creates a {@link org.apache.solr.search.QParser} that can create Spatial {@link org.apache.lucene.search.Filter}s.
 * The filters are tied to implementations of {@link org.apache.solr.schema.SpatialQueryable}
 */
public class SpatialFilterQParserPlugin extends QParserPlugin {
  public static String NAME = "sfilt";


  @Override
  public QParser createParser(String qstr, SolrParams localParams,
                              SolrParams params, SolrQueryRequest req) {

    return new SpatialFilterQParser(qstr, localParams, params, req);
  }

  public void init(NamedList args) {

  }

}

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
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Create a dismax query from the input value.
 * <br>Other parameters: all main query related parameters from the {@link org.apache.solr.handler.DisMaxRequestHandler} are supported.
 * localParams are checked before global request params.
 * <br>Example: <code>{!dismax qf='myfield mytitle^2'}foo</code> creates a dismax query across
 * across myfield and mytitle, with a higher weight on mytitle.
 */
public class DisMaxQParserPlugin extends QParserPlugin {
  public static String NAME = "dismax";

  public void init(NamedList args) {
  }

  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new DisMaxQParser(qstr, localParams, params, req);
  }
}

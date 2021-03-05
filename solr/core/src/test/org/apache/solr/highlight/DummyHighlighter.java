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
package org.apache.solr.highlight;

import org.apache.lucene.search.Query;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocList;

import java.io.IOException;

public class DummyHighlighter extends SolrHighlighter {

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public NamedList<Object> doHighlighting(DocList docs, Query query,
      SolrQueryRequest req, String[] defaultFields) throws IOException {
    NamedList fragments = new SimpleOrderedMap();
    fragments.add("dummy", "thing1");
    return fragments;
  }

}

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
package org.apache.solr.search.mlt;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

/**
 * Usage: {!mlt qf=queryField1,queryField2}uniqueId
 */
public class MLTQParserPlugin extends QParserPlugin {
  public static final String NAME = "mlt";
  
  @Override
  public QParser createParser(String qstr, SolrParams localParams,
      SolrParams params, SolrQueryRequest req) {
    if (req.getCore().getCoreDescriptor().getCloudDescriptor() != null) {
      return new CloudMLTQParser(qstr, localParams, params, req);
    } else {
      return new SimpleMLTQParser(qstr, localParams, params, req);
    }
  }
}

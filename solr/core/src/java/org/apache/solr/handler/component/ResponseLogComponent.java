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
package org.apache.solr.handler.component;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.response.ResultContext;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Adds to the log file the document IDs that are sent in the query response.
 * If document scores are available in the response (by adding the pseudo-
 * column 'score' to the field list) then each document ID will be followed
 * by its score, as in:
 * <pre>
 * "... hits=55 responseLog=22:0.71231794,44:0.61231794 status=0 ..."
 * </pre>
 * 
 * Add it to a requestHandler in solrconfig.xml like this:
 * <pre class="prettyprint">
 * &lt;searchComponent name="responselog" class="solr.ResponseLogComponent"/&gt;
 * 
 * &lt;requestHandler name="/select" class="solr.SearchHandler"&gt;
 *   &lt;lst name="defaults"&gt;
 *   
 *     ...
 *     
 *   &lt;/lst&gt;
 *   &lt;arr name="components"&gt;
 *     &lt;str&gt;responselog&lt;/str&gt;
 *   &lt;/arr&gt;
 * &lt;/requestHandler&gt;</pre>
 *  
 *  It can then be enabled at query time by supplying <pre>responseLog=true</pre>
 *  query parameter.
 */
public class ResponseLogComponent extends SearchComponent {

  public static final String COMPONENT_NAME = "responseLog";

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {}

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false)) return;
    
    SolrIndexSearcher searcher = rb.req.getSearcher();
    IndexSchema schema = searcher.getSchema();
    if (schema.getUniqueKeyField() == null) return;

    ResultContext rc = (ResultContext) rb.rsp.getResponse();

    DocList docs = rc.getDocList();
    if (docs.hasScores()) {
      processScores(rb, docs, schema, searcher);
    } else {
      processIds(rb, docs, schema, searcher);
    }
  }

  protected void processIds(ResponseBuilder rb, DocList dl, IndexSchema schema,
      SolrIndexSearcher searcher) throws IOException {
    
    StringBuilder sb = new StringBuilder();

    Set<String> fields = Collections.singleton(schema.getUniqueKeyField().getName());
    for(DocIterator iter = dl.iterator(); iter.hasNext();) {

      sb.append(schema.printableUniqueKey(searcher.doc(iter.nextDoc(), fields)))
        .append(',');
    }
    if (sb.length() > 0) {
      rb.rsp.addToLog("responseLog", sb.substring(0, sb.length() - 1));
    }  
  }
  
  protected void processScores(ResponseBuilder rb, DocList dl, IndexSchema schema,
      SolrIndexSearcher searcher) throws IOException {
    
    StringBuilder sb = new StringBuilder();
    Set<String> fields = Collections.singleton(schema.getUniqueKeyField().getName());
    for(DocIterator iter = dl.iterator(); iter.hasNext();) {
      sb.append(schema.printableUniqueKey(searcher.doc(iter.nextDoc(), fields)))
        .append(':')
        .append(iter.score())
        .append(',');
    }
    if (sb.length() > 0) {
      rb.rsp.addToLog("responseLog", sb.substring(0, sb.length() - 1));
    }  
  }
  
  @Override
  public String getDescription() {
    return "A component that inserts the retrieved documents (and optionally scores) into the response log entry";
  }
}

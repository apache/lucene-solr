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

package org.apache.solr.handler.component;

import org.apache.lucene.queryParser.ParseException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.SearchHandler;
import org.apache.solr.highlight.SolrHighlighter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;

import java.io.IOException;
import java.net.URL;

/**
 * TODO!
 *
 * @version $Id$
 * @since solr 1.3
 */
public class HighlightComponent extends SearchComponent {
  @Override
  public void prepare(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException, ParseException {
    
  }
  
  @Override
  public void process(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    SolrHighlighter highlighter = req.getCore().getHighlighter();
    if (highlighter.isHighlightingEnabled(req.getParams())) {
      ResponseBuilder builder = SearchHandler.getResponseBuilder( req );
      SolrParams params = req.getParams();

      String[] defaultHighlightFields;  //TODO: get from builder by default?

      if (builder.getQparser() != null) {
        defaultHighlightFields = builder.getQparser().getDefaultHighlightFields();
      } else {
        defaultHighlightFields = params.getParams(CommonParams.DF);
      }
      
      if(builder.getHighlightQuery()==null) {
        if (builder.getQparser() != null) {
          try {
            builder.setHighlightQuery( builder.getQparser().getHighlightQuery() );
          } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
        } else {
          builder.setHighlightQuery( builder.getQuery() );
        }
      }
      
      NamedList sumData = highlighter.doHighlighting(
              builder.getResults().docList,
              builder.getHighlightQuery().rewrite(req.getSearcher().getReader()),
              req, defaultHighlightFields );
      
      if(sumData != null) {
        // TODO ???? add this directly to the response?
        rsp.add("highlighting", sumData);
      }
    }
  }
  
  /////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////
  
  @Override
  public String getDescription() {
    return "Highlighting";
  }
  
  @Override
  public String getVersion() {
    return "$Revision$";
  }
  
  @Override
  public String getSourceId() {
    return "$Id$";
  }
  
  @Override
  public String getSource() {
    return "$URL$";
  }
  
  @Override
  public URL[] getDocs() {
    return null;
  }
}

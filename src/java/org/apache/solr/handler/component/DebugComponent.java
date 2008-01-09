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

import static org.apache.solr.common.params.CommonParams.FQ;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.Query;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.util.SolrPluginUtils;

/**
 * TODO!
 * 
 * @version $Id$
 * @since solr 1.3
 */
public class DebugComponent extends SearchComponent
{
  public static final String COMPONENT_NAME = "debug";
  
  @Override
  public void prepare(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException 
  {
    
  }

  @SuppressWarnings("unchecked")
  @Override
  public void process(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException 
  {
    ResponseBuilder builder = SearchHandler.getResponseBuilder( req );
    if( builder.isDebug() ) {
      NamedList stdinfo = SolrPluginUtils.doStandardDebug( req, 
          builder.getQueryString(), builder.getQuery(), builder.getResults().docList);
      
      NamedList info = builder.getDebugInfo();
      if( info == null ) {
        builder.setDebugInfo( stdinfo );
        info = stdinfo;
      }
      else {
        info.addAll( stdinfo );
      }
      
      if (builder.getQparser() != null) {
        builder.getQparser().addDebugInfo(builder.getDebugInfo());
      }

      if (null != builder.getDebugInfo() ) {
        if (null != builder.getFilters() ) {
          info.add("filter_queries",req.getParams().getParams(FQ));
          List<String> fqs = new ArrayList<String>(builder.getFilters().size());
          for (Query fq : builder.getFilters()) {
            fqs.add(QueryParsing.toString(fq, req.getSchema()));
          }
          info.add("parsed_filter_queries",fqs);
        }
        
        // Add this directly here?
        rsp.add("debug", builder.getDebugInfo() );
      }
    }
  }
  
  /////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "Debug Information";
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

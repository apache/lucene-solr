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
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.SearchHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.search.*;
import org.apache.solr.util.SolrPluginUtils;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO!
 * 
 * @version $Id$
 * @since solr 1.3
 */
public class QueryComponent extends SearchComponent
{
  @Override
  public void prepare(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException, ParseException 
  {
    ResponseBuilder builder = SearchHandler.getResponseBuilder( req );
    SolrParams params = req.getParams();
    
    // Set field flags
    String fl = params.get(CommonParams.FL);
    int fieldFlags = 0;
    if (fl != null) {
      fieldFlags |= SolrPluginUtils.setReturnFields(fl, rsp);
    }
    builder.setFieldFlags( fieldFlags ); 

    String defType = params.get(QueryParsing.DEFTYPE);
    defType = defType==null ? OldLuceneQParserPlugin.NAME : defType;

    if (builder.getQueryString() == null) {
      builder.setQueryString( params.get( CommonParams.Q ) );
    }

    QParser parser = QParser.getParser(builder.getQueryString(), defType, req);
    builder.setQuery( parser.getQuery() );
    builder.setSortSpec( parser.getSort(true) );
    
    String[] fqs = req.getParams().getParams(org.apache.solr.common.params.CommonParams.FQ);
    if (fqs!=null && fqs.length!=0) {
      List<Query> filters = builder.getFilters();
      if (filters==null) {
        filters = new ArrayList<Query>();
        builder.setFilters( filters );
      }
      for (String fq : fqs) {
        if (fq != null && fq.trim().length()!=0) {
          QParser fqp = QParser.getParser(fq, null, req);
          filters.add(fqp.getQuery());
        }
      }
    }
  }

  /**
   * Actually run the query
   */
  @Override
  public void process(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException 
  {
    ResponseBuilder builder = SearchHandler.getResponseBuilder( req );
    SolrIndexSearcher searcher = req.getSearcher();
    SolrParams params = req.getParams();
    
    if( builder.isNeedDocSet() ) {
      builder.setResults( searcher.getDocListAndSet(
          builder.getQuery(), builder.getFilters(), builder.getSortSpec().getSort(),
          builder.getSortSpec().getOffset(), builder.getSortSpec().getCount(),
          builder.getFieldFlags() ) );
    }
    else {
      DocListAndSet results = new DocListAndSet();
      results.docList = searcher.getDocList(
          builder.getQuery(), builder.getFilters(), builder.getSortSpec().getSort(),
          builder.getSortSpec().getOffset(), builder.getSortSpec().getCount(),
          builder.getFieldFlags() );
      builder.setResults( results );
    }

    //pre-fetch returned documents
    if (builder.getResults().docList != null && builder.getResults().docList.size()<=50) {
      // TODO: this may depend on the highlighter component (or other components?)
      SolrPluginUtils.optimizePreFetchDocs(builder.getResults().docList, builder.getQuery(), req, rsp);
    }
    rsp.add("response",builder.getResults().docList);
  }
  

  /////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "query";
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

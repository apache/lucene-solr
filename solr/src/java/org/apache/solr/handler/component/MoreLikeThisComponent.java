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

import java.io.IOException;
import java.net.URL;

import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.MoreLikeThisHandler;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * TODO!
 * 
 *
 * @since solr 1.3
 */
public class MoreLikeThisComponent extends SearchComponent
{
  public static final String COMPONENT_NAME = "mlt";
  
  @Override
  public void prepare(ResponseBuilder rb) throws IOException
  {
    
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException
  {
    SolrParams p = rb.req.getParams();
    if( p.getBool( MoreLikeThisParams.MLT, false ) ) {
      SolrIndexSearcher searcher = rb.req.getSearcher();
      
      NamedList<DocList> sim = getMoreLikeThese( rb, searcher,
          rb.getResults().docList, rb.getFieldFlags() );

      // TODO ???? add this directly to the response?
      rb.rsp.add( "moreLikeThis", sim );
    }
  }

  NamedList<DocList> getMoreLikeThese( ResponseBuilder rb, SolrIndexSearcher searcher,
      DocList docs, int flags ) throws IOException {
    SolrParams p = rb.req.getParams();
    IndexSchema schema = searcher.getSchema();
    MoreLikeThisHandler.MoreLikeThisHelper mltHelper 
      = new MoreLikeThisHandler.MoreLikeThisHelper( p, searcher );
    NamedList<DocList> mlt = new SimpleOrderedMap<DocList>();
    DocIterator iterator = docs.iterator();

    SimpleOrderedMap<Object> dbg = null;
    if( rb.isDebug() ){
      dbg = new SimpleOrderedMap<Object>();
    }

    while( iterator.hasNext() ) {
      int id = iterator.nextDoc();
      int rows = p.getInt( MoreLikeThisParams.DOC_COUNT, 5 );
      DocListAndSet sim = mltHelper.getMoreLikeThis( id, 0, rows, null, null, flags );
      String name = schema.printableUniqueKey( searcher.doc( id ) );
      mlt.add(name, sim.docList);
      
      if( dbg != null ){
        SimpleOrderedMap<Object> docDbg = new SimpleOrderedMap<Object>();
        docDbg.add( "rawMLTQuery", mltHelper.getRawMLTQuery().toString() );
        docDbg.add( "boostedMLTQuery", mltHelper.getBoostedMLTQuery().toString() );
        docDbg.add( "realMLTQuery", mltHelper.getRealMLTQuery().toString() );
        SimpleOrderedMap<Object> explains = new SimpleOrderedMap<Object>();
        DocIterator mltIte = sim.docList.iterator();
        while( mltIte.hasNext() ){
          int mltid = mltIte.nextDoc();
          String key = schema.printableUniqueKey( searcher.doc( mltid ) );
          explains.add( key, searcher.explain( mltHelper.getRealMLTQuery(), mltid ) );
        }
        docDbg.add( "explain", explains );
        dbg.add( name, docDbg );
      }
    }

    // add debug information
    if( dbg != null ){
      rb.addDebugInfo( "moreLikeThis", dbg );
    }
    return mlt;
  }
  
  /////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "More Like This";
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

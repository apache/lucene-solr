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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.QueryElevationParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.QueryElevationComponent.ElevationObj;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;



public class QueryElevationComponentTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-elevate.xml","schema12.xml");
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }
  
  @Test
  public void testInterface() throws Exception
  {
    SolrCore core = h.getCore();
    
    NamedList<String> args = new NamedList<String>();
    args.add( QueryElevationComponent.FIELD_TYPE, "string" );
    args.add( QueryElevationComponent.CONFIG_FILE, "elevate.xml" );
    
    QueryElevationComponent comp = new QueryElevationComponent();
    comp.init( args );
    comp.inform( core );
    
    IndexReader reader = core.getSearcher().get().getReader();
    Map<String, ElevationObj> map = comp.getElevationMap( reader, core );
    // Make sure the boosts loaded properly
    assertEquals( 3, map.size() );
    assertEquals( 1, map.get( "XXXX" ).priority.size() );
    assertEquals( 2, map.get( "YYYY" ).priority.size() );
    assertEquals( 3, map.get( "ZZZZ" ).priority.size() );
    assertEquals( null, map.get( "xxxx" ) );
    assertEquals( null, map.get( "yyyy" ) );
    assertEquals( null, map.get( "zzzz" ) );
    
    // Now test the same thing with a lowercase filter: 'lowerfilt'
    args = new NamedList<String>();
    args.add( QueryElevationComponent.FIELD_TYPE, "lowerfilt" );
    args.add( QueryElevationComponent.CONFIG_FILE, "elevate.xml" );
    
    comp = new QueryElevationComponent();
    comp.init( args );
    comp.inform( core );
    map = comp.getElevationMap( reader, core );
    assertEquals( 3, map.size() );
    assertEquals( null, map.get( "XXXX" ) );
    assertEquals( null, map.get( "YYYY" ) );
    assertEquals( null, map.get( "ZZZZ" ) );
    assertEquals( 1, map.get( "xxxx" ).priority.size() );
    assertEquals( 2, map.get( "yyyy" ).priority.size() );
    assertEquals( 3, map.get( "zzzz" ).priority.size() );
    
    assertEquals( "xxxx", comp.getAnalyzedQuery( "XXXX" ) );
    assertEquals( "xxxxyyyy", comp.getAnalyzedQuery( "XXXX YYYY" ) );
  }

  @Test
  public void testEmptyQuery() throws Exception {
    SolrCore core = h.getCore();

    //String query = "title:ipod";

    Map<String,String> args = new HashMap<String, String>();
    args.put( "q.alt", "*:*" );
    args.put( "defType", "dismax");
    args.put( CommonParams.QT, "/elevate" );
    //args.put( CommonParams.FL, "id,title,score" );
    SolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    assertQ("Make sure QEC handles null queries", req, "//*[@numFound='0']");

  }

  @Test
  public void testSorting() throws IOException
  {
    SolrCore core = h.getCore();
    
    assertU(adoc("id", "a", "title", "ipod",           "str_s", "a" ));
    assertU(adoc("id", "b", "title", "ipod ipod",      "str_s", "b" ));
    assertU(adoc("id", "c", "title", "ipod ipod ipod", "str_s", "c" ));

    assertU(adoc("id", "x", "title", "boosted",                 "str_s", "x" ));
    assertU(adoc("id", "y", "title", "boosted boosted",         "str_s", "y" ));
    assertU(adoc("id", "z", "title", "boosted boosted boosted", "str_s", "z" ));
    assertU(commit());
    
    String query = "title:ipod";
    
    Map<String,String> args = new HashMap<String, String>();
    args.put( CommonParams.Q, query );
    args.put( CommonParams.QT, "/elevate" );
    args.put( CommonParams.FL, "id,score" );
    args.put( "indent", "true" );
    //args.put( CommonParams.FL, "id,title,score" );
    SolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    
    assertQ("Make sure standard sort works as expected", req
            ,"//*[@numFound='3']"
            ,"//result/doc[1]/str[@name='id'][.='a']"
            ,"//result/doc[2]/str[@name='id'][.='b']"
            ,"//result/doc[3]/str[@name='id'][.='c']"
            );
    
    // Explicitly set what gets boosted
    IndexReader reader = core.getSearcher().get().getReader();
    QueryElevationComponent booster = (QueryElevationComponent)core.getSearchComponent( "elevate" );
    booster.elevationCache.clear();
    booster.setTopQueryResults( reader, query, new String[] { "x", "y", "z" }, null );

    assertQ("All six should make it", req
            ,"//*[@numFound='6']"
            ,"//result/doc[1]/str[@name='id'][.='x']"
            ,"//result/doc[2]/str[@name='id'][.='y']"
            ,"//result/doc[3]/str[@name='id'][.='z']"
            ,"//result/doc[4]/str[@name='id'][.='a']"
            ,"//result/doc[5]/str[@name='id'][.='b']"
            ,"//result/doc[6]/str[@name='id'][.='c']"
            );
    
    booster.elevationCache.clear();
    
    // now switch the order:
    booster.setTopQueryResults( reader, query, new String[] { "a", "x" }, null );
    assertQ("All four should make it", req
            ,"//*[@numFound='4']"
            ,"//result/doc[1]/str[@name='id'][.='a']"
            ,"//result/doc[2]/str[@name='id'][.='x']"
            ,"//result/doc[3]/str[@name='id'][.='b']"
            ,"//result/doc[4]/str[@name='id'][.='c']"
            );
    
    // Test reverse sort
    args.put( CommonParams.SORT, "score asc" );
    assertQ("All four should make it", req
        ,"//*[@numFound='4']"
        ,"//result/doc[4]/str[@name='id'][.='a']"
        ,"//result/doc[3]/str[@name='id'][.='x']"
        ,"//result/doc[2]/str[@name='id'][.='b']"
        ,"//result/doc[1]/str[@name='id'][.='c']"
        );
    
    // Try normal sort by 'id'
    // default 'forceBoost' shoudl be false
    assertEquals( false, booster.forceElevation );
    args.put( CommonParams.SORT, "str_s asc" );
    assertQ( null, req
        ,"//*[@numFound='4']"
        ,"//result/doc[1]/str[@name='id'][.='a']"
        ,"//result/doc[2]/str[@name='id'][.='b']"
        ,"//result/doc[3]/str[@name='id'][.='c']"
        ,"//result/doc[4]/str[@name='id'][.='x']"
        );
    
    booster.forceElevation = true;
    assertQ( null, req
        ,"//*[@numFound='4']"
        ,"//result/doc[1]/str[@name='id'][.='a']"
        ,"//result/doc[2]/str[@name='id'][.='x']"
        ,"//result/doc[3]/str[@name='id'][.='b']"
        ,"//result/doc[4]/str[@name='id'][.='c']"
        );

    //Test exclusive (not to be confused with exclusion)
    args.put(QueryElevationParams.EXCLUSIVE, "true");
	booster.setTopQueryResults( reader, query, new String[] { "x", "a" },  new String[] {} );
	assertQ( null, req
	    ,"//*[@numFound='2']"
	    ,"//result/doc[1]/str[@name='id'][.='x']"
	    ,"//result/doc[2]/str[@name='id'][.='a']"            
	    );
    // Test exclusion
    booster.elevationCache.clear();
    args.remove( CommonParams.SORT );
    args.remove( QueryElevationParams.EXCLUSIVE);
    booster.setTopQueryResults( reader, query, new String[] { "x" },  new String[] { "a" } );
    assertQ( null, req
        ,"//*[@numFound='3']"
        ,"//result/doc[1]/str[@name='id'][.='x']"
        ,"//result/doc[2]/str[@name='id'][.='b']"
        ,"//result/doc[3]/str[@name='id'][.='c']"
        );

  }
  
  // write a test file to boost some docs
  private void writeFile( File file, String query, String ... ids ) throws Exception
  {
    PrintWriter out = new PrintWriter( new FileOutputStream( file ) ); 
    out.println( "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" );
    out.println( "<elevate>" );
    out.println( "<query text=\""+query+"\">" );
    for( String id : ids ) {
      out.println( " <doc id=\""+id+"\"/>" );
    }
    out.println( "</query>" );
    out.println( "</elevate>" );
    out.flush();
    out.close();
    
    log.info( "OUT:"+file.getAbsolutePath() );
  }

  @Test
  public void testElevationReloading() throws Exception
  {
    SolrCore core = h.getCore();

    String testfile = "data-elevation.xml";
    File f = new File( core.getDataDir(), testfile );
    writeFile( f, "aaa", "A" );
    
    QueryElevationComponent comp = (QueryElevationComponent)core.getSearchComponent("elevate");
    NamedList<String> args = new NamedList<String>();
    args.add( QueryElevationComponent.CONFIG_FILE, testfile );
    comp.init( args );
    comp.inform( core );
    
    IndexReader reader = core.getSearcher().get().getReader();
    Map<String, ElevationObj> map = comp.getElevationMap(reader, core);
    assertTrue( map.get( "aaa" ).priority.containsKey( "A" ) );
    assertNull( map.get( "bbb" ) );
    
    // now change the file
    writeFile( f, "bbb", "B" );
    assertU(adoc("id", "10000")); // will get same reader if no index change
    assertU(commit());
    
    reader = core.getSearcher().get().getReader();
    map = comp.getElevationMap(reader, core);
    assertNull( map.get( "aaa" ) );
    assertTrue( map.get( "bbb" ).priority.containsKey( "B" ) );
  }
}

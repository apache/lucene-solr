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
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.logging.Logger;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreDocComparator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortComparatorSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.Config;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SortSpec;
import org.apache.solr.util.VersionedFile;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.solr.request.SolrQueryRequest;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * A component to elevate some documents to the top of the result set.
 * 
 * @version $Id$
 * @since solr 1.3
 */
public class QueryElevationComponent extends SearchComponent implements SolrCoreAware
{
  private static Logger log = Logger.getLogger(QueryElevationComponent.class.getName());
  
  // Constants used in solrconfig.xml
  static final String FIELD_TYPE = "queryFieldType";
  static final String CONFIG_FILE = "config-file";
  static final String FORCE_ELEVATION = "forceElevation";
  static final String EXCLUDE = "exclude";
  
  // Runtime param -- should be in common?
  static final String ENABLE = "enableElevation";
    
  private SolrParams initArgs = null;
  private Analyzer analyzer = null;
  private String idField = null;
  boolean forceElevation = false;
  
  // For each IndexReader, keep a query->elevation map
  // When the configuration is loaded from the data directory.
  // The key is null if loaded from the config directory, and
  // is never re-loaded.
  final Map<IndexReader,Map<String, ElevationObj>> elevationCache = 
    new WeakHashMap<IndexReader, Map<String,ElevationObj>>();

  class ElevationObj {
    final String text;
    final String analyzed;
    final BooleanClause[] exclude;
    final BooleanQuery include;
    final Map<String,Integer> priority;
    
    ElevationObj( String qstr, List<String> elevate, List<String> exclude ) throws IOException
    {
      this.text = qstr;
      this.analyzed = getAnalyzedQuery( this.text );
      
      this.include = new BooleanQuery();
      this.include.setBoost( 0 );
      this.priority = new HashMap<String, Integer>();
      int max = elevate.size()+5;
      for( String id : elevate ) {
        TermQuery tq = new TermQuery( new Term( idField, id ) );
        include.add( tq, BooleanClause.Occur.SHOULD );
        this.priority.put( id, max-- );
      }
      
      if( exclude == null || exclude.isEmpty() ) {
        this.exclude = null;
      }
      else {
        this.exclude = new BooleanClause[exclude.size()];
        for( int i=0; i<exclude.size(); i++ ) {
          TermQuery tq = new TermQuery( new Term( idField, exclude.get(i) ) );
          this.exclude[i] = new BooleanClause( tq, BooleanClause.Occur.MUST_NOT );
        }
      }
    }
  }
  
  @Override
  public void init( NamedList args )
  {
    this.initArgs = SolrParams.toSolrParams( args );
  }
  
  public void inform(SolrCore core)
  {
    String a = initArgs.get( FIELD_TYPE );
    if( a != null ) {
      FieldType ft = core.getSchema().getFieldTypes().get( a );
      if( ft == null ) {
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
            "Unknown FieldType: '"+a+"' used in QueryElevationComponent" );
      }
      analyzer = ft.getAnalyzer();
    }

    SchemaField sf = core.getSchema().getUniqueKeyField();
    if( sf == null ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
          "QueryElevationComponent requires the schema to have a uniqueKeyField" );
    }
    idField = sf.getName().intern();
    
    forceElevation = initArgs.getBool( FORCE_ELEVATION, forceElevation );
    try {
      synchronized( elevationCache ) {
        elevationCache.clear();
        String f = initArgs.get( CONFIG_FILE );
        if( f == null ) {
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
              "QueryElevationComponent must specify argument: '"+CONFIG_FILE
              +"' -- path to elevate.xml" );
        }
        File fC = new File( core.getResourceLoader().getConfigDir(), f );
        File fD = new File( core.getDataDir(), f );
        if( fC.exists() == fD.exists() ) {
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
              "QueryElevationComponent missing config file: '"+f + "\n"
              +"either: "+fC.getAbsolutePath() + " or " + fD.getAbsolutePath() + " must exist, but not both." );
        }
        if( fC.exists() ) {
          log.info( "Loading QueryElevation from: "+fC.getAbsolutePath() );
          Config cfg = new Config( core.getResourceLoader(), f );
          elevationCache.put(null, loadElevationMap( cfg ));
        }
        else {
          // preload the first data
          IndexReader reader = core.getSearcher().get().getReader(); 
          getElevationMap( reader, core );
        }
      }
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "Error initializing QueryElevationComponent.", ex );
    }
  }

  Map<String, ElevationObj> getElevationMap( IndexReader reader, SolrCore core ) throws Exception
  {
    synchronized( elevationCache ) {
      Map<String, ElevationObj> map = elevationCache.get( null );
      if (map != null) return map;

      map = elevationCache.get( reader );
      if( map == null ) {
        String f = initArgs.get( CONFIG_FILE );
        if( f == null ) {
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
                  "QueryElevationComponent must specify argument: "+CONFIG_FILE );
        }
        log.info( "Loading QueryElevation from data dir: "+f );

        InputStream is = VersionedFile.getLatestFile( core.getDataDir(), f );
        Config cfg = new Config( core.getResourceLoader(), f, is, null );
        map = loadElevationMap( cfg );
        elevationCache.put( reader, map );
      }
      return map;
    }
  }
  
  private Map<String, ElevationObj> loadElevationMap( Config cfg ) throws IOException
  {
    XPath xpath = XPathFactory.newInstance().newXPath();
    Map<String, ElevationObj> map = new HashMap<String, ElevationObj>();
    NodeList nodes = (NodeList)cfg.evaluate( "elevate/query", XPathConstants.NODESET );
    for (int i=0; i<nodes.getLength(); i++) {
      Node node = nodes.item( i );
      String qstr = DOMUtil.getAttr( node, "text", "missing query 'text'" );
      
      NodeList children = null;
      try {
        children = (NodeList)xpath.evaluate("doc", node, XPathConstants.NODESET);
      } 
      catch (XPathExpressionException e) {
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
            "query requires '<doc .../>' child" );
      }

      ArrayList<String> include = new ArrayList<String>();
      ArrayList<String> exclude = new ArrayList<String>();
      for (int j=0; j<children.getLength(); j++) {
        Node child = children.item(j);
        String id = DOMUtil.getAttr( child, "id", "missing 'id'" );
        String e = DOMUtil.getAttr( child, EXCLUDE, null );
        if( e != null ) {
          if( Boolean.valueOf( e ) ) {
            exclude.add( id );
            continue;
          }
        }
        include.add( id );
      }
      
      ElevationObj elev = new ElevationObj( qstr, include, exclude );
      if( map.containsKey( elev.analyzed ) ) {
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
            "Boosting query defined twice for query: '"+elev.text+"' ("+elev.analyzed+"')" );
      }
      map.put( elev.analyzed, elev );
    }
    return map;
  }
  
  /**
   * Helpful for testing without loading config.xml
   * @throws IOException 
   */
  void setTopQueryResults( IndexReader reader, String query, String[] ids, String[] ex ) throws IOException
  {
    if( ids == null ) {
      ids = new String[0];
    }
    if( ex == null ) {
      ex = new String[0];
    }
    
    Map<String,ElevationObj> elev = elevationCache.get( reader );
    if( elev == null ) {
      elev = new HashMap<String, ElevationObj>();
      elevationCache.put( reader, elev );
    }
    ElevationObj obj = new ElevationObj( query, Arrays.asList(ids), Arrays.asList(ex) );
    elev.put( obj.analyzed, obj );
  }
  
  String getAnalyzedQuery( String query ) throws IOException
  {
    if( analyzer == null ) {
      return query;
    }
    StringBuilder norm = new StringBuilder();
    TokenStream tokens = analyzer.tokenStream( null, new StringReader( query ) );
    Token token = tokens.next();
    while( token != null ) {
      norm.append( token.termText() );
      token = tokens.next();
    }
    return norm.toString();
  }

  //---------------------------------------------------------------------------------
  // SearchComponent
  //---------------------------------------------------------------------------------
  
  @Override
  public void prepare(ResponseBuilder rb) throws IOException
  {
    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams();
    // A runtime param can skip 
    if( !params.getBool( ENABLE, true ) ) {
      return;
    }
    
    Query query = rb.getQuery();
    if( query == null ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "The QueryElevationComponent needs to be registered 'after' the query component" );
    }
    
    String qstr = getAnalyzedQuery( rb.getQueryString() );
    IndexReader reader = req.getSearcher().getReader();
    ElevationObj booster = null;
    try {
      booster = getElevationMap( reader, req.getCore() ).get( qstr );
    }
    catch( Exception ex ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "Error loading elevation", ex );      
    }
    
    if( booster != null ) {
      // Change the query to insert forced documents
      BooleanQuery newq = new BooleanQuery( true );
      newq.add( query, BooleanClause.Occur.SHOULD );
      newq.add( booster.include, BooleanClause.Occur.SHOULD );
      if( booster.exclude != null ) {
        for( BooleanClause bq : booster.exclude ) {
          newq.add( bq );
        }
      }
      rb.setQuery( newq );
      
      // if the sort is 'score desc' use a custom sorting method to 
      // insert documents in their proper place 
      SortSpec sortSpec = rb.getSortSpec();
      if( sortSpec.getSort() == null ) {
        sortSpec.setSort( new Sort( new SortField[] {
            new SortField(idField, new ElevationComparatorSource(booster.priority), false ),
            new SortField(null, SortField.SCORE, false)
        }));
      }
      else {
        // Check if the sort is based on score
        boolean modify = false;
        SortField[] current = sortSpec.getSort().getSort();
        ArrayList<SortField> sorts = new ArrayList<SortField>( current.length + 1 );
        // Perhaps force it to always sort by score
        if( forceElevation && current[0].getType() != SortField.SCORE ) {
          sorts.add( new SortField(idField, 
              new ElevationComparatorSource(booster.priority), false ) );
          modify = true;
        }
        for( SortField sf : current ) {
          if( sf.getType() == SortField.SCORE ) {
            sorts.add( new SortField(idField, 
                new ElevationComparatorSource(booster.priority), sf.getReverse() ) );
            modify = true;
          }
          sorts.add( sf );
        }
        if( modify ) {
          sortSpec.setSort( new Sort( sorts.toArray( new SortField[sorts.size()] ) ) );
        }
      }
    }
    
    // Add debugging information
    if( rb.isDebug() ) {
      List<String> match = null;
      if( booster != null ) {
        // Extract the elevated terms into a list
        match = new ArrayList<String>(booster.priority.size());
        for( Object o : booster.include.clauses() ) {
          TermQuery tq = (TermQuery)((BooleanClause)o).getQuery();
          match.add( tq.getTerm().text() );
        }
      }
      
      SimpleOrderedMap<Object> dbg = new SimpleOrderedMap<Object>();
      dbg.add( "q", qstr );
      dbg.add( "match", match );
      rb.addDebugInfo( "queryBoosting", dbg );
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    // Do nothing -- the real work is modifying the input query
  }
    
  //---------------------------------------------------------------------------------
  // SolrInfoMBean
  //---------------------------------------------------------------------------------

  @Override
  public String getDescription() {
    return "Query Boosting -- boost particular documents for a given query";
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
    try {
      return new URL[] {
        new URL("http://wiki.apache.org/solr/QueryElevationComponent")
      };
    } 
    catch (MalformedURLException e) {
      throw new RuntimeException( e );
    }
  }
}

/**
 * Comparator source that knows about elevated documents
 */
class ElevationComparatorSource implements SortComparatorSource 
{
  private final Map<String,Integer> priority;
  
  public ElevationComparatorSource( final Map<String,Integer> boosts) {
    this.priority = boosts;
  }
  
  public ScoreDocComparator newComparator(final IndexReader reader, final String fieldname)
    throws IOException 
  {

    // A future alternate version could store internal docids (would need to be regenerated per IndexReader)
    // instead of loading the FieldCache instance into memory.

    final FieldCache.StringIndex index =
            FieldCache.DEFAULT.getStringIndex(reader, fieldname);
  
    return new ScoreDocComparator () 
    {
      public final int compare (final ScoreDoc d0, final ScoreDoc d1) {
        final int f0 = index.order[d0.doc];
        final int f1 = index.order[d1.doc];
 
        final String id0 = index.lookup[f0];
        final String id1 = index.lookup[f1];
 
        final Integer b0 = priority.get( id0 );
        final Integer b1 = priority.get( id1 );

        final int v0 = (b0 == null) ? -1 : b0.intValue();
        final int v1 = (b1 == null) ? -1 : b1.intValue();
       
        return v1 - v0;
      }
  
      public Comparable sortValue (final ScoreDoc d0) {
        final int f0 = index.order[d0.doc];
        final String id0 = index.lookup[f0];
        final Integer b0 = priority.get( id0 );
        final int v0 = (b0 == null) ? -1 : b0.intValue();       
        return new Integer( v0 );
      }
  
      public int sortType() {
        return SortField.CUSTOM;
      }
    };
  }
}



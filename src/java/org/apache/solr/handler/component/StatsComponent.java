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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.FieldCache;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.StatsParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Stats component calculates simple statistics on numeric field values
 * 
 * @version $Id$
 * @since solr 1.4
 */
public class StatsComponent extends SearchComponent {

  public static final String COMPONENT_NAME = "stats";
  
  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    if (rb.req.getParams().getBool(StatsParams.STATS,false)) {
      rb.setNeedDocSet( true );
      rb.doStats = true;
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    if (rb.doStats) {
      SolrParams params = rb.req.getParams();
      SimpleStats s = new SimpleStats(rb.req,
              rb.getResults().docSet,
              params );

      // TODO ???? add this directly to the response, or to the builder?
      rb.rsp.add( "stats", s.getStatsCounts() );
    }
  }

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    return ResponseBuilder.STAGE_DONE;
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    if (!rb.doStats) return;

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
        sreq.purpose |= ShardRequest.PURPOSE_GET_STATS;

        StatsInfo si = rb._statsInfo;
        if (si == null) {
          rb._statsInfo = si = new StatsInfo();
          si.parse(rb.req.getParams(), rb);
          // should already be true...
          // sreq.params.set(StatsParams.STATS, "true");
        }
    } else {
      // turn off stats on other requests
      sreq.params.set(StatsParams.STATS, "false");
      // we could optionally remove stats params
    }
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if (!rb.doStats || (sreq.purpose & ShardRequest.PURPOSE_GET_STATS)==0) return;

    StatsInfo si = rb._statsInfo;

    for (ShardResponse srsp: sreq.responses) {
      NamedList stats = (NamedList)srsp.getSolrResponse().getResponse().get("stats");

      NamedList stats_fields = (NamedList)stats.get("stats_fields");
      if (stats_fields != null) {
        for (int i=0; i<stats_fields.size(); i++) {
          String field = stats_fields.getName(i);
          StatsValues stv = si.statsFields.get(field);
          stv.accumulate( (NamedList)stats_fields.get(field) );
        }
      }
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (!rb.doStats || rb.stage != ResponseBuilder.STAGE_GET_FIELDS) return;
    // wait until STAGE_GET_FIELDS
    // so that "result" is already stored in the response (for aesthetics)
    
    StatsInfo si = rb._statsInfo;
    
    NamedList stats = new SimpleOrderedMap();
    NamedList stats_fields = new SimpleOrderedMap();
    stats.add("stats_fields",stats_fields);
    for(String field : si.statsFields.keySet()){
      stats_fields.add(field, si.statsFields.get(field).getStatsValues());
    }

    rb.rsp.add("stats", stats);

    rb._statsInfo = null;
  }


  /////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "Calculate Statistics";
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

}

class StatsInfo {
  Map<String, StatsValues> statsFields;

  void parse(SolrParams params, ResponseBuilder rb) {
    statsFields = new HashMap<String, StatsValues>();

    String[] statsFs = params.getParams(StatsParams.STATS_FIELD);
    if (statsFs != null) {
      for (String field : statsFs) {
        statsFields.put(field,new StatsValues());
      }
    }
  }
}

class StatsValues {
  private static final String FACETS = "facets";
  double min;
  double max;
  double sum;
  double sumOfSquares;
  long count;
  long missing;
  Double median = null;
  
  // facetField   facetValue
  Map<String, Map<String,StatsValues>> facets;
  
  public StatsValues() {
    reset();
  }

  public void accumulate(NamedList stv){
    min = Math.min(min, (Double)stv.get("min"));
    max = Math.max(max, (Double)stv.get("max"));
    sum += (Double)stv.get("sum");
    count += (Long)stv.get("count");
    missing += (Long)stv.get("missing");
    sumOfSquares += (Double)stv.get("sumOfSquares");
    
    NamedList f = (NamedList)stv.get( FACETS );
    if( f != null ) {
      if( facets == null ) {
        facets = new HashMap<String, Map<String,StatsValues>>();
      }
      
      for( int i=0; i< f.size(); i++ ) {
        String field = f.getName(i);
        NamedList vals = (NamedList)f.getVal( i );
        Map<String,StatsValues> addTo = facets.get( field );
        if( addTo == null ) {
          addTo = new HashMap<String,StatsValues>();
          facets.put( field, addTo );
        }
        for( int j=0; j< vals.size(); j++ ) {
          String val = f.getName(i);
          StatsValues vvals = addTo.get( val );
          if( vvals == null ) {
            vvals = new StatsValues();
            addTo.put( val, vvals );
          }
          vvals.accumulate( (NamedList)f.getVal( i ) );
        }
      }
    }
  }

  public void accumulate(double v){
    sumOfSquares += (v*v); // for std deviation
    min = Math.min(min, v);
    max = Math.max(max, v);
    sum += v;
    count++;
  }
  
  public double getAverage(){
    return sum / count;
  }
  
  public double getStandardDeviation()
  {
    if( count <= 1.0D ) 
      return 0.0D;
    
    return Math.sqrt( ( ( count * sumOfSquares ) - ( sum * sum ) )
                      / ( count * ( count - 1.0D ) ) );  
  }
  
  public void reset(){
    min = Double.MAX_VALUE;
    max = Double.MIN_VALUE;
    sum = count = missing = 0;
    sumOfSquares = 0;
    median = null;
    facets = null;
  }
  
  public NamedList<?> getStatsValues(){
    NamedList<Object> res = new SimpleOrderedMap<Object>();
    res.add("min", min);
    res.add("max", max);
    res.add("sum", sum);
    res.add("count", count);
    res.add("missing", missing);
    res.add("sumOfSquares", sumOfSquares );
    res.add("mean", getAverage());
    if( median != null ) {
      res.add( "median", median );
    }
    res.add( "stddev", getStandardDeviation() );
    
    // add the facet stats
    if( facets != null && facets.size() > 0 ) {
      NamedList<NamedList<?>> nl = new SimpleOrderedMap<NamedList<?>>();
      for( Map.Entry<String, Map<String,StatsValues>> entry : facets.entrySet() ) {
        NamedList<NamedList<?>> nl2 = new SimpleOrderedMap<NamedList<?>>();
        nl.add( entry.getKey(), nl2 );
        for( Map.Entry<String, StatsValues> e2 : entry.getValue().entrySet() ) {
          nl2.add( e2.getKey(), e2.getValue().getStatsValues() );
        }
      }
      res.add( FACETS, nl );
    }
    return res;
  }
}

class FieldFacetStats {
  final String name;
  final FieldCache.StringIndex si;
  final FieldType ft;

  final String[] terms;
  final int[] termNum;
  
  final int startTermIndex;
  final int endTermIndex;
  final int nTerms;
  
  final Map<String,StatsValues> facetStatsValues;
  
  FieldFacetStats( String name, FieldCache.StringIndex si, FieldType ft )
  {
    this.name = name;
    this.si = si;
    this.ft = ft;
    
    terms = si.lookup;
    termNum = si.order;
    startTermIndex = 1;
    endTermIndex = terms.length;
    nTerms = endTermIndex - startTermIndex;
    
    facetStatsValues = new HashMap<String, StatsValues>();
  }
  
  String getTermText( int docID )
  {
    return terms[termNum[docID]];
  }
  
  public boolean facet( int docID, Double v )
  {
    if( v == null ) return false;
    
    int term = termNum[docID];
    int arrIdx = term-startTermIndex;
    if (arrIdx>=0 && arrIdx<nTerms) {
      String key = ft.indexedToReadable( terms[term] );
      StatsValues stats = facetStatsValues.get( key );
      if( stats == null ) {
        stats = new StatsValues();
        facetStatsValues.put(key, stats);
      }
      stats.accumulate( v );
      return true;
    }
    return false;
  }
}

class SimpleStats {

  /** The main set of documents */
  protected DocSet docs;
  /** Configuration params behavior should be driven by */
  protected SolrParams params;
  /** Searcher to use for all calculations */
  protected SolrIndexSearcher searcher;
  protected SolrQueryRequest req;

  public SimpleStats(SolrQueryRequest req,
                      DocSet docs,
                      SolrParams params) {
    this.req = req;
    this.searcher = req.getSearcher();
    this.docs = docs;
    this.params = params;
  }

  public NamedList<Object> getStatsCounts() {
    NamedList<Object> res = new SimpleOrderedMap<Object>();
    res.add("stats_fields", getStatsFields());
    return res;
  }

  public NamedList getStatsFields() {
    NamedList<NamedList<Number>> res = new SimpleOrderedMap<NamedList<Number>>();
    String[] statsFs = params.getParams(StatsParams.STATS_FIELD);
    if (null != statsFs) {
      for (String f : statsFs) {
        String[] facets = params.getFieldParams( f, StatsParams.STATS_FACET );
        if( facets == null ) {
          facets = new String[0]; // make sure it is something...
        }
        res.add(f, getFieldCacheStats(f, facets));
      }
    }
    return res;
  }
  
  public NamedList getFieldCacheStats(String fieldName, String[] facet ) {
    FieldType ft = searcher.getSchema().getFieldType(fieldName);
    if( ft.isTokenized() || ft.isMultiValued() ) {
      throw new SolrException( ErrorCode.BAD_REQUEST, 
          "Stats are valid for single valued numeric values.  not: "+fieldName + "["+ft+"]" );
    }

    FieldCache.StringIndex si = null;
    try {
      si = FieldCache.DEFAULT.getStringIndex(searcher.getReader(), fieldName);
    } 
    catch (IOException e) {
      throw new RuntimeException( "failed to open field cache for: "+fieldName, e );
    }
    FieldFacetStats all = new FieldFacetStats( "all", si, ft );
    if ( all.nTerms <= 0 || docs.size() <= 0 ) return null;
    StatsValues allstats = new StatsValues();

    // don't worry about faceting if the no documents match...
    int i=0;
    final FieldFacetStats[] finfo = new FieldFacetStats[facet.length];
    for( String f : facet ) {
      ft = searcher.getSchema().getFieldType(f);
      if( ft.isTokenized() || ft.isMultiValued() ) {
        throw new SolrException( ErrorCode.BAD_REQUEST, 
            "Stats can only facet on single valued fields, not: "+f + "["+ft+"]" );
      }
      try {
        si = FieldCache.DEFAULT.getStringIndex(searcher.getReader(), f);
      } 
      catch (IOException e) {
        throw new RuntimeException( "failed to open field cache for: "+f, e );
      }
      finfo[i++] = new FieldFacetStats( f, si, ft );
    }
    
    
    DocIterator iter = docs.iterator();
    while (iter.hasNext()) {
      int docID = iter.nextDoc();
      String raw = all.getTermText(docID);
      Double v = null;
      if( raw != null ) {
        v = Double.parseDouble( all.ft.indexedToReadable(raw) );
        allstats.accumulate( v );
      }
      else {
        allstats.missing++;
      }
      
      // now check the facets
      for( FieldFacetStats f : finfo ) {
        f.facet(docID, v);
      }
    }
    
    // Find things that require a 2nd pass
    if( params.getFieldBool(fieldName, StatsParams.STATS_TWOPASS, false) ) {
      if( allstats.count > 1 ) { // must be 2 or more...
        iter = docs.iterator();
        boolean isEven = ( allstats.count % 2) == 0;
        int medianIndex = (int) Math.ceil( allstats.count/2.0 );
        for ( i=0; iter.hasNext(); ) {
          String raw = all.getTermText(iter.nextDoc());
          if( raw != null ) {
            if( ++i == medianIndex ) {
              double val0 = Double.parseDouble(  all.ft.indexedToReadable(raw) );
              if( isEven ) {
                do {
                  raw = all.getTermText(iter.nextDoc());
                } while( raw == null );
                double val1 = Double.parseDouble(  all.ft.indexedToReadable(raw) );
                allstats.median = (val0+val1)/2.0;
              }
              else {
                allstats.median = val0;
              }
              break;
            }
          }
        }
      } // get median
    }
    
    if( finfo.length > 0 ) {
      allstats.facets = new HashMap<String, Map<String,StatsValues>>();
      for( FieldFacetStats f : finfo ) {
        allstats.facets.put( f.name, f.facetStatsValues );
      }
    }
    return allstats.getStatsValues();
  }
}

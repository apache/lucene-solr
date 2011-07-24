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

package org.apache.solr.client.solrj.response;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;

import java.util.*;

/**
 * 
 *
 * @since solr 1.3
 */
@SuppressWarnings("unchecked")
public class QueryResponse extends SolrResponseBase 
{
  // Direct pointers to known types
  private NamedList<Object> _header = null;
  private SolrDocumentList _results = null;
  private NamedList<ArrayList> _sortvalues = null;
  private NamedList<Object> _facetInfo = null;
  private NamedList<Object> _debugInfo = null;
  private NamedList<Object> _highlightingInfo = null;
  private NamedList<NamedList<Object>> _spellInfo = null;
  private NamedList<Object> _statsInfo = null;
  private NamedList<NamedList<Number>> _termsInfo = null;

  // Facet stuff
  private Map<String,Integer> _facetQuery = null;
  private List<FacetField> _facetFields = null;
  private List<FacetField> _limitingFacets = null;
  private List<FacetField> _facetDates = null;
  private List<RangeFacet> _facetRanges = null;
  private NamedList<List<PivotField>> _facetPivot = null;

  // Highlight Info
  private Map<String,Map<String,List<String>>> _highlighting = null;

  // SpellCheck Response
  private SpellCheckResponse _spellResponse = null;

  // Terms Response
  private TermsResponse _termsResponse = null;
  
  // Field stats Response
  private Map<String,FieldStatsInfo> _fieldStatsInfo = null;
  
  // Debug Info
  private Map<String,Object> _debugMap = null;
  private Map<String,String> _explainMap = null;

  // utility variable used for automatic binding -- it should not be serialized
  private transient final SolrServer solrServer;
  
  public QueryResponse(){
    solrServer = null;
  }
  
  /**
   * Utility constructor to set the solrServer and namedList
   */
  public QueryResponse( NamedList<Object> res , SolrServer solrServer){
    this.setResponse( res );
    this.solrServer = solrServer;
  }

  @Override
  public void setResponse( NamedList<Object> res )
  {
    super.setResponse( res );
    
    // Look for known things
    for( int i=0; i<res.size(); i++ ) {
      String n = res.getName( i );
      if( "responseHeader".equals( n ) ) {
        _header = (NamedList<Object>) res.getVal( i );
      }
      else if( "response".equals( n ) ) {
        _results = (SolrDocumentList) res.getVal( i );
      }
      else if( "sort_values".equals( n ) ) {
        _sortvalues = (NamedList<ArrayList>) res.getVal( i );
      }
      else if( "facet_counts".equals( n ) ) {
        _facetInfo = (NamedList<Object>) res.getVal( i );
        // extractFacetInfo inspects _results, so defer calling it
        // in case it hasn't been populated yet.
      }
      else if( "debug".equals( n ) ) {
        _debugInfo = (NamedList<Object>) res.getVal( i );
        extractDebugInfo( _debugInfo );
      }
      else if( "highlighting".equals( n ) ) {
        _highlightingInfo = (NamedList<Object>) res.getVal( i );
        extractHighlightingInfo( _highlightingInfo );
      }
      else if ( "spellcheck".equals( n ) )  {
        _spellInfo = (NamedList<NamedList<Object>>) res.getVal( i );
        extractSpellCheckInfo( _spellInfo );
      }
      else if ( "stats".equals( n ) )  {
        _statsInfo = (NamedList<Object>) res.getVal( i );
        extractStatsInfo( _statsInfo );
      }
      else if ( "terms".equals( n ) ) {
        _termsInfo = (NamedList<NamedList<Number>>) res.getVal( i );
        extractTermsInfo( _termsInfo );
      }
    }
    if(_facetInfo != null) extractFacetInfo( _facetInfo );
  }

  private void extractSpellCheckInfo(NamedList<NamedList<Object>> spellInfo) {
    _spellResponse = new SpellCheckResponse(spellInfo);
  }

  private void extractTermsInfo(NamedList<NamedList<Number>> termsInfo) {
    _termsResponse = new TermsResponse(termsInfo);
  }
  
  private void extractStatsInfo(NamedList<Object> info) {
    if( info != null ) {
      _fieldStatsInfo = new HashMap<String, FieldStatsInfo>();
      NamedList<NamedList<Object>> ff = (NamedList<NamedList<Object>>) info.get( "stats_fields" );
      if( ff != null ) {
        for( Map.Entry<String,NamedList<Object>> entry : ff ) {
          NamedList<Object> v = entry.getValue();
          if( v != null ) {
            _fieldStatsInfo.put( entry.getKey(), 
                new FieldStatsInfo( v, entry.getKey() ) );
          }
        }
      }
    }
  }

  private void extractDebugInfo( NamedList<Object> debug )
  {
    _debugMap = new LinkedHashMap<String, Object>(); // keep the order
    for( Map.Entry<String, Object> info : debug ) {
      _debugMap.put( info.getKey(), info.getValue() );
    }

    // Parse out interesting bits from the debug info
    _explainMap = new HashMap<String, String>();
    NamedList<String> explain = (NamedList<String>)_debugMap.get( "explain" );
    if( explain != null ) {
      for( Map.Entry<String, String> info : explain ) {
        String key = info.getKey();
        _explainMap.put( key, info.getValue() );
      }
    }
  }

  private void extractHighlightingInfo( NamedList<Object> info )
  {
    _highlighting = new HashMap<String,Map<String,List<String>>>();
    for( Map.Entry<String, Object> doc : info ) {
      Map<String,List<String>> fieldMap = new HashMap<String, List<String>>();
      _highlighting.put( doc.getKey(), fieldMap );
      
      NamedList<List<String>> fnl = (NamedList<List<String>>)doc.getValue();
      for( Map.Entry<String, List<String>> field : fnl ) {
        fieldMap.put( field.getKey(), field.getValue() );
      }
    }
  }

  private void extractFacetInfo( NamedList<Object> info )
  {
    // Parse the queries
    _facetQuery = new LinkedHashMap<String, Integer>();
    NamedList<Integer> fq = (NamedList<Integer>) info.get( "facet_queries" );
    if (fq != null) {
      for( Map.Entry<String, Integer> entry : fq ) {
        _facetQuery.put( entry.getKey(), entry.getValue() );
      }
    }
    
    // Parse the facet info into fields
    // TODO?? The list could be <int> or <long>?  If always <long> then we can switch to <Long>
    NamedList<NamedList<Number>> ff = (NamedList<NamedList<Number>>) info.get( "facet_fields" );
    if( ff != null ) {
      _facetFields = new ArrayList<FacetField>( ff.size() );
      _limitingFacets = new ArrayList<FacetField>( ff.size() );
      
      long minsize = _results == null ? Long.MAX_VALUE :_results.getNumFound();
      for( Map.Entry<String,NamedList<Number>> facet : ff ) {
        FacetField f = new FacetField( facet.getKey() );
        for( Map.Entry<String, Number> entry : facet.getValue() ) {
          f.add( entry.getKey(), entry.getValue().longValue() );
        }
        
        _facetFields.add( f );
        FacetField nl = f.getLimitingFields( minsize );
        if( nl.getValueCount() > 0 ) {
          _limitingFacets.add( nl );
        }
      }
    }
    
    //Parse date facets
    NamedList<NamedList<Object>> df = (NamedList<NamedList<Object>>) info.get("facet_dates");
    if (df != null) {
      // System.out.println(df);
      _facetDates = new ArrayList<FacetField>( df.size() );
      for (Map.Entry<String, NamedList<Object>> facet : df) {
        // System.out.println("Key: " + facet.getKey() + " Value: " + facet.getValue());
        NamedList<Object> values = facet.getValue();
        String gap = (String) values.get("gap");
        Date end = (Date) values.get("end");
        FacetField f = new FacetField(facet.getKey(), gap, end);
        
        for (Map.Entry<String, Object> entry : values)   {
          try {
            f.add(entry.getKey(), Long.parseLong(entry.getValue().toString()));
          } catch (NumberFormatException e) {
            //Ignore for non-number responses which are already handled above
          }
        }
        
        _facetDates.add(f);
      }
    }

    //Parse range facets
    NamedList<NamedList<Object>> rf = (NamedList<NamedList<Object>>) info.get("facet_ranges");
    if (rf != null) {
      _facetRanges = new ArrayList<RangeFacet>( rf.size() );
      for (Map.Entry<String, NamedList<Object>> facet : rf) {
        NamedList<Object> values = facet.getValue();
        Object rawGap = values.get("gap");

        RangeFacet rangeFacet;
        if (rawGap instanceof Number) {
          Number gap = (Number) rawGap;
          Number start = (Number) values.get("start");
          Number end = (Number) values.get("end");
          rangeFacet = new RangeFacet.Numeric(facet.getKey(), start, end, gap);
        } else {
          String gap = (String) rawGap;
          Date start = (Date) values.get("start");
          Date end = (Date) values.get("end");
          rangeFacet = new RangeFacet.Date(facet.getKey(), start, end, gap);
        }

        NamedList<Integer> counts = (NamedList<Integer>) values.get("counts");
        for (Map.Entry<String, Integer> entry : counts)   {
          rangeFacet.addCount(entry.getKey(), entry.getValue());
        }

        _facetRanges.add(rangeFacet);
      }
    }
    
    //Parse pivot facets
    NamedList pf = (NamedList) info.get("facet_pivot");
    if (pf != null) {
      _facetPivot = new NamedList<List<PivotField>>();
      for( int i=0; i<pf.size(); i++ ) {
        _facetPivot.add( pf.getName(i), readPivots( (List<NamedList>)pf.getVal(i) ) );
      }
    }
  }
  
  protected List<PivotField> readPivots( List<NamedList> list )
  {
    ArrayList<PivotField> values = new ArrayList<PivotField>( list.size() );
    for( NamedList nl : list ) {
      // NOTE, this is cheating, but we know the order they are written in, so no need to check
      String f = (String)nl.getVal( 0 );
      Object v = nl.getVal( 1 );
      int cnt = ((Integer)nl.getVal( 2 )).intValue();
      List<PivotField> p = (nl.size()<4)?null:readPivots((List<NamedList>)nl.getVal(3) );
      values.add( new PivotField( f, v, cnt, p ) );
    }
    return values;
  }

  //------------------------------------------------------
  //------------------------------------------------------

  /**
   * Remove the field facet info
   */
  public void removeFacets() {
    _facetFields = new ArrayList<FacetField>();
  }
  
  //------------------------------------------------------
  //------------------------------------------------------

  public NamedList<Object> getHeader() {
    return _header;
  }

  public SolrDocumentList getResults() {
    return _results;
  }
 
  public NamedList<ArrayList> getSortValues(){
    return _sortvalues;
  }

  public Map<String, Object> getDebugMap() {
    return _debugMap;
  }

  public Map<String, String> getExplainMap() {
    return _explainMap;
  }

  public Map<String,Integer> getFacetQuery() {
    return _facetQuery;
  }

  public Map<String, Map<String, List<String>>> getHighlighting() {
    return _highlighting;
  }

  public SpellCheckResponse getSpellCheckResponse() {
    return _spellResponse;
  }

  public TermsResponse getTermsResponse() {
    return _termsResponse;
  }
  
  /**
   * See also: {@link #getLimitingFacets()}
   */
  public List<FacetField> getFacetFields() {
    return _facetFields;
  }
  
  public List<FacetField> getFacetDates()   {
    return _facetDates;
  }

  public List<RangeFacet> getFacetRanges() {
    return _facetRanges;
  }

  public NamedList<List<PivotField>> getFacetPivot()   {
    return _facetPivot;
  }
  
  /** get 
   * 
   * @param name the name of the 
   * @return the FacetField by name or null if it does not exist
   */
  public FacetField getFacetField(String name) {
    if (_facetFields==null) return null;
    for (FacetField f : _facetFields) {
      if (f.getName().equals(name)) return f;
    }
    return null;
  }
  
  public FacetField getFacetDate(String name)   {
    if (_facetDates == null)
      return null;
    for (FacetField f : _facetDates)
      if (f.getName().equals(name))
        return f;
    return null;
  }
  
  /**
   * @return a list of FacetFields where the count is less then
   * then #getResults() {@link SolrDocumentList#getNumFound()}
   * 
   * If you want all results exactly as returned by solr, use:
   * {@link #getFacetFields()}
   */
  public List<FacetField> getLimitingFacets() {
    return _limitingFacets;
  }
  
  public <T> List<T> getBeans(Class<T> type){
    return solrServer == null ? 
      new DocumentObjectBinder().getBeans(type,_results):
      solrServer.getBinder().getBeans(type, _results);
  }

  public Map<String, FieldStatsInfo> getFieldStatsInfo() {
    return _fieldStatsInfo;
  }
}




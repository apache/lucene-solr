package org.apache.lucene.search.vectorhighlight;
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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.vectorhighlight.FieldTermStack.TermInfo;

/**
 * FieldQuery breaks down query object into terms/phrases and keeps
 * them in a QueryPhraseMap structure.
 */
public class FieldQuery {

  final boolean fieldMatch;

  // fieldMatch==true,  Map<fieldName,QueryPhraseMap>
  // fieldMatch==false, Map<null,QueryPhraseMap>
  Map<String, QueryPhraseMap> rootMaps = new HashMap<String, QueryPhraseMap>();

  // fieldMatch==true,  Map<fieldName,setOfTermsInQueries>
  // fieldMatch==false, Map<null,setOfTermsInQueries>
  Map<String, Set<String>> termSetMap = new HashMap<String, Set<String>>();

  int termOrPhraseNumber; // used for colored tag support

  // The maximum number of different matching terms accumulated from any one MultiTermQuery
  private static final int MAX_MTQ_TERMS = 1024;

  FieldQuery( Query query, IndexReader reader, boolean phraseHighlight, boolean fieldMatch ) throws IOException {
    this.fieldMatch = fieldMatch;
    Set<Query> flatQueries = new LinkedHashSet<Query>();
    flatten( query, reader, flatQueries );
    saveTerms( flatQueries, reader );
    Collection<Query> expandQueries = expand( flatQueries );

    for( Query flatQuery : expandQueries ){
      QueryPhraseMap rootMap = getRootMap( flatQuery );
      rootMap.add( flatQuery, reader );
      if( !phraseHighlight && flatQuery instanceof PhraseQuery ){
        PhraseQuery pq = (PhraseQuery)flatQuery;
        if( pq.getTerms().length > 1 ){
          for( Term term : pq.getTerms() )
            rootMap.addTerm( term, flatQuery.getBoost() );
        }
      }
    }
  }
  
  /** For backwards compatibility you can initialize FieldQuery without
   * an IndexReader, which is only required to support MultiTermQuery
   */
  FieldQuery( Query query, boolean phraseHighlight, boolean fieldMatch ) throws IOException {
    this (query, null, phraseHighlight, fieldMatch);
  }

  void flatten( Query sourceQuery, IndexReader reader, Collection<Query> flatQueries ) throws IOException{
    if( sourceQuery instanceof BooleanQuery ){
      BooleanQuery bq = (BooleanQuery)sourceQuery;
      for( BooleanClause clause : bq.getClauses() ){
        if( !clause.isProhibited() )
          flatten( clause.getQuery(), reader, flatQueries );
      }
    } else if( sourceQuery instanceof DisjunctionMaxQuery ){
      DisjunctionMaxQuery dmq = (DisjunctionMaxQuery)sourceQuery;
      for( Query query : dmq ){
        flatten( query, reader, flatQueries );
      }
    }
    else if( sourceQuery instanceof TermQuery ){
      if( !flatQueries.contains( sourceQuery ) )
        flatQueries.add( sourceQuery );
    }
    else if( sourceQuery instanceof PhraseQuery ){
      if( !flatQueries.contains( sourceQuery ) ){
        PhraseQuery pq = (PhraseQuery)sourceQuery;
        if( pq.getTerms().length > 1 )
          flatQueries.add( pq );
        else if( pq.getTerms().length == 1 ){
          flatQueries.add( new TermQuery( pq.getTerms()[0] ) );
        }
      }
    } else if (sourceQuery instanceof ConstantScoreQuery) {
      final Query q = ((ConstantScoreQuery) sourceQuery).getQuery();
      if (q != null) {
        flatten(q, reader, flatQueries);
      }
    } else if (sourceQuery instanceof FilteredQuery) {
      final Query q = ((FilteredQuery) sourceQuery).getQuery();
      if (q != null) {
        flatten(q, reader, flatQueries);
      }
    } else if (reader != null){
      Query query = sourceQuery;
      if (sourceQuery instanceof MultiTermQuery) {
        MultiTermQuery copy = (MultiTermQuery) sourceQuery.clone();
        copy.setRewriteMethod(new MultiTermQuery.TopTermsScoringBooleanQueryRewrite(MAX_MTQ_TERMS));
        query = copy;
      }
      Query rewritten = query.rewrite(reader);
      if (rewritten != query) {
        // only rewrite once and then flatten again - the rewritten query could have a speacial treatment
        // if this method is overwritten in a subclass.
        flatten(rewritten, reader, flatQueries);
        
      } 
      // if the query is already rewritten we discard it
    }
    // else discard queries
  }
  
  /*
   * Create expandQueries from flatQueries.
   * 
   * expandQueries := flatQueries + overlapped phrase queries
   * 
   * ex1) flatQueries={a,b,c}
   *      => expandQueries={a,b,c}
   * ex2) flatQueries={a,"b c","c d"}
   *      => expandQueries={a,"b c","c d","b c d"}
   */
  Collection<Query> expand( Collection<Query> flatQueries ){
    Set<Query> expandQueries = new LinkedHashSet<Query>();
    for( Iterator<Query> i = flatQueries.iterator(); i.hasNext(); ){
      Query query = i.next();
      i.remove();
      expandQueries.add( query );
      if( !( query instanceof PhraseQuery ) ) continue;
      for( Iterator<Query> j = flatQueries.iterator(); j.hasNext(); ){
        Query qj = j.next();
        if( !( qj instanceof PhraseQuery ) ) continue;
        checkOverlap( expandQueries, (PhraseQuery)query, (PhraseQuery)qj );
      }
    }
    return expandQueries;
  }

  /*
   * Check if PhraseQuery A and B have overlapped part.
   * 
   * ex1) A="a b", B="b c" => overlap; expandQueries={"a b c"}
   * ex2) A="b c", B="a b" => overlap; expandQueries={"a b c"}
   * ex3) A="a b", B="c d" => no overlap; expandQueries={}
   */
  private void checkOverlap( Collection<Query> expandQueries, PhraseQuery a, PhraseQuery b ){
    if( a.getSlop() != b.getSlop() ) return;
    Term[] ats = a.getTerms();
    Term[] bts = b.getTerms();
    if( fieldMatch && !ats[0].field().equals( bts[0].field() ) ) return;
    checkOverlap( expandQueries, ats, bts, a.getSlop(), a.getBoost() );
    checkOverlap( expandQueries, bts, ats, b.getSlop(), b.getBoost() );
  }

  /*
   * Check if src and dest have overlapped part and if it is, create PhraseQueries and add expandQueries.
   * 
   * ex1) src="a b", dest="c d"       => no overlap
   * ex2) src="a b", dest="a b c"     => no overlap
   * ex3) src="a b", dest="b c"       => overlap; expandQueries={"a b c"}
   * ex4) src="a b c", dest="b c d"   => overlap; expandQueries={"a b c d"}
   * ex5) src="a b c", dest="b c"     => no overlap
   * ex6) src="a b c", dest="b"       => no overlap
   * ex7) src="a a a a", dest="a a a" => overlap;
   *                                     expandQueries={"a a a a a","a a a a a a"}
   * ex8) src="a b c d", dest="b c"   => no overlap
   */
  private void checkOverlap( Collection<Query> expandQueries, Term[] src, Term[] dest, int slop, float boost ){
    // beginning from 1 (not 0) is safe because that the PhraseQuery has multiple terms
    // is guaranteed in flatten() method (if PhraseQuery has only one term, flatten()
    // converts PhraseQuery to TermQuery)
    for( int i = 1; i < src.length; i++ ){
      boolean overlap = true;
      for( int j = i; j < src.length; j++ ){
        if( ( j - i ) < dest.length && !src[j].text().equals( dest[j-i].text() ) ){
          overlap = false;
          break;
        }
      }
      if( overlap && src.length - i < dest.length ){
        PhraseQuery pq = new PhraseQuery();
        for( Term srcTerm : src )
          pq.add( srcTerm );
        for( int k = src.length - i; k < dest.length; k++ ){
          pq.add( new Term( src[0].field(), dest[k].text() ) );
        }
        pq.setSlop( slop );
        pq.setBoost( boost );
        if(!expandQueries.contains( pq ) )
          expandQueries.add( pq );
      }
    }
  }
  
  QueryPhraseMap getRootMap( Query query ){
    String key = getKey( query );
    QueryPhraseMap map = rootMaps.get( key );
    if( map == null ){
      map = new QueryPhraseMap( this );
      rootMaps.put( key, map );
    }
    return map;
  }
  
  /*
   * Return 'key' string. 'key' is the field name of the Query.
   * If not fieldMatch, 'key' will be null.
   */
  private String getKey( Query query ){
    if( !fieldMatch ) return null;
    if( query instanceof TermQuery )
      return ((TermQuery)query).getTerm().field();
    else if ( query instanceof PhraseQuery ){
      PhraseQuery pq = (PhraseQuery)query;
      Term[] terms = pq.getTerms();
      return terms[0].field();
    }
    else if (query instanceof MultiTermQuery) {
      return ((MultiTermQuery)query).getField();
    }
    else
      throw new RuntimeException( "query \"" + query.toString() + "\" must be flatten first." );
  }

  /*
   * Save the set of terms in the queries to termSetMap.
   * 
   * ex1) q=name:john
   *      - fieldMatch==true
   *          termSetMap=Map<"name",Set<"john">>
   *      - fieldMatch==false
   *          termSetMap=Map<null,Set<"john">>
   *          
   * ex2) q=name:john title:manager
   *      - fieldMatch==true
   *          termSetMap=Map<"name",Set<"john">,
   *                         "title",Set<"manager">>
   *      - fieldMatch==false
   *          termSetMap=Map<null,Set<"john","manager">>
   *          
   * ex3) q=name:"john lennon"
   *      - fieldMatch==true
   *          termSetMap=Map<"name",Set<"john","lennon">>
   *      - fieldMatch==false
   *          termSetMap=Map<null,Set<"john","lennon">>
   */
    void saveTerms( Collection<Query> flatQueries, IndexReader reader ) throws IOException{
    for( Query query : flatQueries ){
      Set<String> termSet = getTermSet( query );
      if( query instanceof TermQuery )
        termSet.add( ((TermQuery)query).getTerm().text() );
      else if( query instanceof PhraseQuery ){
        for( Term term : ((PhraseQuery)query).getTerms() )
          termSet.add( term.text() );
      }
      else if (query instanceof MultiTermQuery && reader != null) {
        BooleanQuery mtqTerms = (BooleanQuery) query.rewrite(reader);
        for (BooleanClause clause : mtqTerms.getClauses()) {
          termSet.add (((TermQuery) clause.getQuery()).getTerm().text());
        }
      }
      else
        throw new RuntimeException( "query \"" + query.toString() + "\" must be flatten first." );
    }
  }
  
  private Set<String> getTermSet( Query query ){
    String key = getKey( query );
    Set<String> set = termSetMap.get( key );
    if( set == null ){
      set = new HashSet<String>();
      termSetMap.put( key, set );
    }
    return set;
  }
  
  Set<String> getTermSet( String field ){
    return termSetMap.get( fieldMatch ? field : null );
  }

  /**
   * 
   * @return QueryPhraseMap
   */
  public QueryPhraseMap getFieldTermMap( String fieldName, String term ){
    QueryPhraseMap rootMap = getRootMap( fieldName );
    return rootMap == null ? null : rootMap.subMap.get( term );
  }

  /**
   * 
   * @return QueryPhraseMap
   */
  public QueryPhraseMap searchPhrase( String fieldName, final List<TermInfo> phraseCandidate ){
    QueryPhraseMap root = getRootMap( fieldName );
    if( root == null ) return null;
    return root.searchPhrase( phraseCandidate );
  }
  
  private QueryPhraseMap getRootMap( String fieldName ){
    return rootMaps.get( fieldMatch ? fieldName : null );
  }
  
  int nextTermOrPhraseNumber(){
    return termOrPhraseNumber++;
  }
  
  /**
   * Internal structure of a query for highlighting: represents
   * a nested query structure
   */
  public static class QueryPhraseMap {

    boolean terminal;
    int slop;   // valid if terminal == true and phraseHighlight == true
    float boost;  // valid if terminal == true
    int termOrPhraseNumber;   // valid if terminal == true
    FieldQuery fieldQuery;
    Map<String, QueryPhraseMap> subMap = new HashMap<String, QueryPhraseMap>();
    
    public QueryPhraseMap( FieldQuery fieldQuery ){
      this.fieldQuery = fieldQuery;
    }

    void addTerm( Term term, float boost ){
      QueryPhraseMap map = getOrNewMap( subMap, term.text() );
      map.markTerminal( boost );
    }
    
    private QueryPhraseMap getOrNewMap( Map<String, QueryPhraseMap> subMap, String term ){
      QueryPhraseMap map = subMap.get( term );
      if( map == null ){
        map = new QueryPhraseMap( fieldQuery );
        subMap.put( term, map );
      }
      return map;
    }

      void add( Query query, IndexReader reader ) {
      if( query instanceof TermQuery ){
        addTerm( ((TermQuery)query).getTerm(), query.getBoost() );
      }
      else if( query instanceof PhraseQuery ){
        PhraseQuery pq = (PhraseQuery)query;
        Term[] terms = pq.getTerms();
        Map<String, QueryPhraseMap> map = subMap;
        QueryPhraseMap qpm = null;
        for( Term term : terms ){
          qpm = getOrNewMap( map, term.text() );
          map = qpm.subMap;
        }
        qpm.markTerminal( pq.getSlop(), pq.getBoost() );
      }
      else
        throw new RuntimeException( "query \"" + query.toString() + "\" must be flatten first." );
    }
    
    public QueryPhraseMap getTermMap( String term ){
      return subMap.get( term );
    }
    
    private void markTerminal( float boost ){
      markTerminal( 0, boost );
    }
    
    private void markTerminal( int slop, float boost ){
      this.terminal = true;
      this.slop = slop;
      this.boost = boost;
      this.termOrPhraseNumber = fieldQuery.nextTermOrPhraseNumber();
    }
    
    public boolean isTerminal(){
      return terminal;
    }
    
    public int getSlop(){
      return slop;
    }
    
    public float getBoost(){
      return boost;
    }
    
    public int getTermOrPhraseNumber(){
      return termOrPhraseNumber;
    }
    
    public QueryPhraseMap searchPhrase( final List<TermInfo> phraseCandidate ){
      QueryPhraseMap currMap = this;
      for( TermInfo ti : phraseCandidate ){
        currMap = currMap.subMap.get( ti.getText() );
        if( currMap == null ) return null;
      }
      return currMap.isValidTermOrPhrase( phraseCandidate ) ? currMap : null;
    }
    
    public boolean isValidTermOrPhrase( final List<TermInfo> phraseCandidate ){
      // check terminal
      if( !terminal ) return false;

      // if the candidate is a term, it is valid
      if( phraseCandidate.size() == 1 ) return true;

      // else check whether the candidate is valid phrase
      // compare position-gaps between terms to slop
      int pos = phraseCandidate.get( 0 ).getPosition();
      for( int i = 1; i < phraseCandidate.size(); i++ ){
        int nextPos = phraseCandidate.get( i ).getPosition();
        if( Math.abs( nextPos - pos - 1 ) > slop ) return false;
        pos = nextPos;
      }
      return true;
    }
  }
}

/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.request;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.*;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrException;
import org.apache.solr.request.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.request.DefaultSolrParams;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.*;
import org.apache.solr.util.NamedList;
import org.apache.solr.util.BoundedTreeSet;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

/**
 * A class that generates simple Facet information for a request.
 *
 * More advanced facet implementations may compose or subclass this class 
 * to leverage any of it's functionality.
 */
public class SimpleFacets {

  /** The main set of documents all facet counts should be relative to */
  protected DocSet docs;
  /** Configuration params behavior should be driven by */
  protected SolrParams params;
  /** Searcher to use for all calculations */
  protected SolrIndexSearcher searcher;
  
  public SimpleFacets(SolrIndexSearcher searcher, 
                      DocSet docs, 
                      SolrParams params) {
    this.searcher = searcher;
    this.docs = docs;
    this.params = params;
  }
  
  /**
   * Looks at various Params to determing if any simple Facet Constraint count
   * computations are desired.
   *
   * @see #getFacetQueryCounts
   * @see #getFacetFieldCounts
   * @see SolrParams#FACET
   * @return a NamedList of Facet Count info or null
   */
  public NamedList getFacetCounts() {

    // if someone called this method, benefit of the doubt: assume true
    if (!params.getBool(params.FACET,true)) 
      return null;

    NamedList res = new NamedList();
    try {

      res.add("facet_queries", getFacetQueryCounts());

      res.add("facet_fields", getFacetFieldCounts());
      
    } catch (Exception e) {
      SolrException.logOnce(SolrCore.log, "Exception during facet counts", e);
      res.add("exception", SolrException.toStr(e));
    }
    return res;
  }

  /**
   * Returns a list of facet counts for each of the facet queries 
   * specified in the params
   *
   * @see SolrParams#FACET_QUERY
   */
  public NamedList getFacetQueryCounts() throws IOException,ParseException {
    
    NamedList res = new NamedList();

    /* Ignore SolrParams.DF - could have init param facet.query assuming
     * the schema default with query param DF intented to only affect Q.
     * If user doesn't want schema default for facet.query, they should be
     * explicit.
     */
    SolrQueryParser qp = new SolrQueryParser(searcher.getSchema(),null);
    
    String[] facetQs = params.getParams(SolrParams.FACET_QUERY);
    if (null != facetQs && 0 != facetQs.length) {
      for (String q : facetQs) {
        res.add(q, searcher.numDocs(qp.parse(q), docs));
      }
    }

    return res;
  }

  /**
   * Returns a list of value constraints and the associated facet counts 
   * for each facet field specified in the params.
   *
   * @see SolrParams#FACET_FIELD
   * @see #getFacetFieldMissingCount
   * @see #getFacetTermEnumCounts
   */
  public NamedList getFacetFieldCounts() 
    throws IOException {
    
    NamedList res = new NamedList();
    String[] facetFs = params.getParams(SolrParams.FACET_FIELD);
    if (null != facetFs && 0 != facetFs.length) {
      
      for (String f : facetFs) {

        NamedList counts = getFacetTermEnumCounts(f);
        
        if (params.getFieldBool(f, params.FACET_MISSING, false))
          counts.add(null, getFacetFieldMissingCount(f));
        
        res.add(f, counts);
        
      }
    }
    return res;
  }

  /**
   * Returns a count of the documents in the set which do not have any 
   * terms for for the specified field.
   *
   * @see SolrParams#FACET_MISSING
   */
  public int getFacetFieldMissingCount(String fieldName)
    throws IOException {

    DocSet hasVal = searcher.getDocSet
      (new ConstantScoreRangeQuery(fieldName, null, null, false, false));
    return docs.andNotSize(hasVal);
  }

  /**
   * Returns a list of terms in the specified field along with the 
   * corrisponding count of documents in the set that match that constraint.
   *
   * @see SolrParams#FACET_LIMIT
   * @see SolrParams#FACET_ZEROS
   */
  public NamedList getFacetTermEnumCounts(String fieldName) 
    throws IOException {
    
    /* :TODO: potential optimization...
     * cache the Terms with the highest docFreq and try them first
     * don't enum if we get our max from them
     */
     
    IndexSchema schema = searcher.getSchema();
    IndexReader r = searcher.getReader();
    FieldType ft = schema.getFieldType(fieldName);

    Set<CountPair<String,Integer>> counts 
      = new HashSet<CountPair<String,Integer>>();

    String limit = params.getFieldParam(fieldName, params.FACET_LIMIT);
    if (null != limit) {
      counts = new BoundedTreeSet<CountPair<String,Integer>>
        (Integer.parseInt(limit));
    }

    boolean zeros = params.getFieldBool(fieldName, params.FACET_ZEROS, true);
      
    TermEnum te = r.terms(new Term(fieldName,""));
    do {
      Term t = te.term();

      if (null == t || ! t.field().equals(fieldName)) 
        break;

      if (0 < te.docFreq()) { /* all docs may be deleted */
        int count = searcher.numDocs(new TermQuery(t),
                                     docs);

        /* :TODO: is indexedToReadable correct? */ 
        if (zeros || 0 < count) 
          counts.add(new CountPair<String,Integer>
                     (ft.indexedToReadable(t.text()), count));

      }
    } while (te.next());

    NamedList res = new NamedList();
    for (CountPair<String,Integer> p : counts) {
      res.add(p.key, p.val);
    }
    return res;
  }

  /**
   * A simple key=>val pair whose natural order is such that 
   * <b>higher</b> vals come before lower vals.
   * In case of tie vals, then <b>lower</b> keys come before higher keys.
   */
  public static class CountPair<K extends Comparable<? super K>, V extends Comparable<? super V>> 
    implements Comparable<CountPair<K,V>> {

    public CountPair(K k, V v) {
      key = k; val = v;
    }
    public K key;
    public V val;
    public int hashCode() {
      return key.hashCode() ^ val.hashCode();
    }
    public boolean equals(Object o) {
      return (o instanceof CountPair) 
        && (0 == this.compareTo((CountPair<K,V>) o));
    }
    public int compareTo(CountPair<K,V> o) {
      int vc = o.val.compareTo(val);
      return (0 != vc ? vc : key.compareTo(o.key));
    }
  }
}


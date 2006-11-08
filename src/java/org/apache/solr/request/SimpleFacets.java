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

package org.apache.solr.request;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.*;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrException;
import org.apache.solr.request.SolrParams;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.BoolField;
import org.apache.solr.search.*;
import org.apache.solr.util.NamedList;
import org.apache.solr.util.BoundedTreeSet;

import java.io.IOException;
import java.util.*;

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


  public NamedList getTermCounts(String field) throws IOException {
    int limit = params.getFieldInt(field, params.FACET_LIMIT, 100);
    boolean zeros = params.getFieldBool(field, params.FACET_ZEROS, true);
    boolean missing = params.getFieldBool(field, params.FACET_MISSING, false);

    NamedList counts;
    SchemaField sf = searcher.getSchema().getField(field);
    FieldType ft = sf.getType();
    if (sf.multiValued() || ft.isTokenized() || ft instanceof BoolField) {
      // Always use filters for booleans... we know the number of values is very small.
      counts = getFacetTermEnumCounts(searcher,docs,field,limit,zeros,missing);
    } else {
      // TODO: future logic could use filters instead of the fieldcache if
      // the number of terms in the field is small enough.
      counts = getFieldCacheCounts(searcher, docs, field, limit, zeros, missing);
    }

    return counts;
  }


  /**
   * Returns a list of value constraints and the associated facet counts 
   * for each facet field specified in the params.
   *
   * @see SolrParams#FACET_FIELD
   * @see #getFieldMissingCount
   * @see #getFacetTermEnumCounts
   */
  public NamedList getFacetFieldCounts()
          throws IOException {

    NamedList res = new NamedList();
    String[] facetFs = params.getParams(SolrParams.FACET_FIELD);
    if (null != facetFs) {
      for (String f : facetFs) {
        res.add(f, getTermCounts(f));
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
  public static int getFieldMissingCount(SolrIndexSearcher searcher, DocSet docs, String fieldName)
    throws IOException {

    DocSet hasVal = searcher.getDocSet
      (new ConstantScoreRangeQuery(fieldName, null, null, false, false));
    return docs.andNotSize(hasVal);
  }

  /**
   * Use the Lucene FieldCache to get counts for each unique field value in <code>docs</code>.
   * The field must have at most one indexed token per document.
   */
  public static NamedList getFieldCacheCounts(SolrIndexSearcher searcher, DocSet docs, String fieldName, int limit, boolean zeros, boolean missing) throws IOException {
    // TODO: If the number of terms is high compared to docs.size(), and zeros==false,
    //  we should use an alternate strategy to avoid
    //  1) creating another huge int[] for the counts
    //  2) looping over that huge int[] looking for the rare non-zeros.
    //
    // Yet another variation: if docs.size() is small and termvectors are stored,
    // then use them instead of the FieldCache.
    //

    FieldCache.StringIndex si = FieldCache.DEFAULT.getStringIndex(searcher.getReader(), fieldName);
    int[] count = new int[si.lookup.length];
    DocIterator iter = docs.iterator();
    while (iter.hasNext()) {
      count[si.order[iter.nextDoc()]]++;
    }

    FieldType ft = searcher.getSchema().getFieldType(fieldName);
    NamedList res = new NamedList();

    // IDEA: we could also maintain a count of "other"... everything that fell outside
    // of the top 'N'

    BoundedTreeSet<CountPair<String,Integer>> queue=null;

    if (limit>=0) {
      // TODO: compare performance of BoundedTreeSet compare against priority queue?
      queue = new BoundedTreeSet<CountPair<String,Integer>>(limit);
    }

    int min=-1;  // the smallest value in the top 'N' values
    for (int i=1; i<count.length; i++) {
      int c = count[i];
      if (c==0 && !zeros) continue;
      if (limit<0) {
        res.add(ft.indexedToReadable(si.lookup[i]), c);
      } else if (c>min) {
        // NOTE: we use c>min rather than c>=min as an optimization because we are going in
        // index order, so we already know that the keys are ordered.  This can be very
        // important if a lot of the counts are repeated (like zero counts would be).
        queue.add(new CountPair<String,Integer>(ft.indexedToReadable(si.lookup[i]), c));
        if (queue.size()>=limit) min=queue.last().val;
      }
    }

    if (limit>=0) {
      for (CountPair<String,Integer> p : queue) {
        res.add(p.key, p.val);
      }
    }


    if (missing) res.add(null, count[0]);
    return res;
  }

  /**
   * Returns a list of terms in the specified field along with the 
   * corrisponding count of documents in the set that match that constraint.
   * This method uses the FilterCache to get the intersection count between <code>docs</code>
   * and the DocSet for each term in the filter.
   *
   * @see SolrParams#FACET_LIMIT
   * @see SolrParams#FACET_ZEROS
   * @see SolrParams#FACET_MISSING
   */
  public NamedList getFacetTermEnumCounts(SolrIndexSearcher searcher, DocSet docs, String field, int limit, boolean zeros, boolean missing)
    throws IOException {

    /* :TODO: potential optimization...
    * cache the Terms with the highest docFreq and try them first
    * don't enum if we get our max from them
    */

    IndexSchema schema = searcher.getSchema();
    IndexReader r = searcher.getReader();
    FieldType ft = schema.getFieldType(field);

    Set<CountPair<String,Integer>> counts
      = new HashSet<CountPair<String,Integer>>();

    if (0 <= limit) {
      counts = new BoundedTreeSet<CountPair<String,Integer>>(limit);
    }

    TermEnum te = r.terms(new Term(field,""));
    do {
      Term t = te.term();

      if (null == t || ! t.field().equals(field))
        break;

      if (0 < te.docFreq()) { /* all docs may be deleted */
        int count = searcher.numDocs(new TermQuery(t),
                                     docs);

        if (zeros || 0 < count)
          counts.add(new CountPair<String,Integer>
                     (t.text(), count));

      }
    } while (te.next());

    NamedList res = new NamedList();
    for (CountPair<String,Integer> p : counts) {
      res.add(ft.indexedToReadable(p.key), p.val);
    }

    if (missing) {
      res.add(null, getFieldMissingCount(searcher,docs,field));
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


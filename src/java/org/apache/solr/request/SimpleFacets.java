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
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams.FacetDateOther;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.DateField;
import org.apache.solr.search.*;
import org.apache.solr.util.BoundedTreeSet;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.handler.component.ResponseBuilder;

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
  protected SolrQueryRequest req;
  protected ResponseBuilder rb;

  // per-facet values
  SolrParams localParams; // localParams on this particular facet command
  String facetValue;      // the field to or query to facet on (minus local params)
  DocSet base;            // the base docset for this particular facet
  String key;             // what name should the results be stored under

  public SimpleFacets(SolrQueryRequest req,
                      DocSet docs,
                      SolrParams params) {
    this(req,docs,params,null);
  }

  public SimpleFacets(SolrQueryRequest req,
                      DocSet docs,
                      SolrParams params,
                      ResponseBuilder rb) {
    this.req = req;
    this.searcher = req.getSearcher();
    this.base = this.docs = docs;
    this.params = params;
    this.rb = rb;
  }


  void parseParams(String type, String param) throws ParseException, IOException {
    localParams = QueryParsing.getLocalParams(param, req.getParams());
    base = docs;
    facetValue = param;
    key = param;

    if (localParams == null) return;

    // remove local params unless it's a query
    if (type != FacetParams.FACET_QUERY) {
      facetValue = localParams.get(CommonParams.VALUE);
    }

    // reset set the default key now that localParams have been removed
    key = facetValue;

    // allow explicit set of the key
    key = localParams.get(CommonParams.OUTPUT_KEY, key);

    // figure out if we need a new base DocSet
    String excludeStr = localParams.get(CommonParams.EXCLUDE);
    if (excludeStr == null) return;

    Map tagMap = (Map)req.getContext().get("tags");
    if (tagMap != null && rb != null) {
      List<String> excludeTagList = StrUtils.splitSmart(excludeStr,',');

      IdentityHashMap<Query,Boolean> excludeSet = new IdentityHashMap<Query,Boolean>();
      for (String excludeTag : excludeTagList) {
        Object olst = tagMap.get(excludeTag);
        // tagMap has entries of List<String,List<QParser>>, but subject to change in the future
        if (!(olst instanceof Collection)) continue;
        for (Object o : (Collection)olst) {
          if (!(o instanceof QParser)) continue;
          QParser qp = (QParser)o;
          excludeSet.put(qp.getQuery(), Boolean.TRUE);
        }
      }
      if (excludeSet.size() == 0) return;

      List<Query> qlist = new ArrayList<Query>();

      // add the base query
      qlist.add(rb.getQuery());

      // add the filters
      for (Query q : rb.getFilters()) {
        if (!excludeSet.containsKey(q)) {
          qlist.add(q);
        }

      }

      // get the new base docset for this facet
      base = searcher.getDocSet(qlist);
    }

  }


  /**
   * Looks at various Params to determing if any simple Facet Constraint count
   * computations are desired.
   *
   * @see #getFacetQueryCounts
   * @see #getFacetFieldCounts
   * @see #getFacetDateCounts
   * @see FacetParams#FACET
   * @return a NamedList of Facet Count info or null
   */
  public NamedList getFacetCounts() {

    // if someone called this method, benefit of the doubt: assume true
    if (!params.getBool(FacetParams.FACET,true))
      return null;

    NamedList res = new SimpleOrderedMap();
    try {

      res.add("facet_queries", getFacetQueryCounts());
      res.add("facet_fields", getFacetFieldCounts());
      res.add("facet_dates", getFacetDateCounts());
      
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
   * @see FacetParams#FACET_QUERY
   */
  public NamedList getFacetQueryCounts() throws IOException,ParseException {

    NamedList res = new SimpleOrderedMap();

    /* Ignore SolrParams.DF - could have init param facet.query assuming
     * the schema default with query param DF intented to only affect Q.
     * If user doesn't want schema default for facet.query, they should be
     * explicit.
     */
    // SolrQueryParser qp = searcher.getSchema().getSolrQueryParser(null);

    String[] facetQs = params.getParams(FacetParams.FACET_QUERY);
    if (null != facetQs && 0 != facetQs.length) {
      for (String q : facetQs) {
        parseParams(FacetParams.FACET_QUERY, q);

        // TODO: slight optimization would prevent double-parsing of any localParams
        Query qobj = QParser.getParser(q, null, req).getQuery();
        res.add(key, searcher.numDocs(qobj, base));
      }
    }

    return res;
  }


  public NamedList getTermCounts(String field) throws IOException {
    int offset = params.getFieldInt(field, FacetParams.FACET_OFFSET, 0);
    int limit = params.getFieldInt(field, FacetParams.FACET_LIMIT, 100);
    if (limit == 0) return new NamedList();
    Integer mincount = params.getFieldInt(field, FacetParams.FACET_MINCOUNT);
    if (mincount==null) {
      Boolean zeros = params.getFieldBool(field, FacetParams.FACET_ZEROS);
      // mincount = (zeros!=null && zeros) ? 0 : 1;
      mincount = (zeros!=null && !zeros) ? 1 : 0;
      // current default is to include zeros.
    }
    boolean missing = params.getFieldBool(field, FacetParams.FACET_MISSING, false);
    // default to sorting if there is a limit.
    String sort = params.getFieldParam(field, FacetParams.FACET_SORT, limit>0 ? FacetParams.FACET_SORT_COUNT : FacetParams.FACET_SORT_INDEX);
    String prefix = params.getFieldParam(field,FacetParams.FACET_PREFIX);


    NamedList counts;
    SchemaField sf = searcher.getSchema().getField(field);
    FieldType ft = sf.getType();

    // determine what type of faceting method to use
    String method = params.getFieldParam(field, FacetParams.FACET_METHOD);
    boolean enumMethod = FacetParams.FACET_METHOD_enum.equals(method);
    if (method == null && ft instanceof BoolField) {
      // Always use filters for booleans... we know the number of values is very small.
      enumMethod = true;
    }
    boolean multiToken = sf.multiValued() || ft.isTokenized();

    // unless the enum method is explicitly specified, use a counting method.
    if (enumMethod) {
      counts = getFacetTermEnumCounts(searcher, base, field, offset, limit, mincount,missing,sort,prefix);
    } else {
      if (multiToken) {
        UnInvertedField uif = UnInvertedField.getUnInvertedField(field, searcher);
        counts = uif.getCounts(searcher, base, offset, limit, mincount,missing,sort,prefix);
      } else {
        // TODO: future logic could use filters instead of the fieldcache if
        // the number of terms in the field is small enough.
        counts = getFieldCacheCounts(searcher, base, field, offset,limit, mincount, missing, sort, prefix);
      }
    }

    return counts;
  }


  /**
   * Returns a list of value constraints and the associated facet counts 
   * for each facet field specified in the params.
   *
   * @see FacetParams#FACET_FIELD
   * @see #getFieldMissingCount
   * @see #getFacetTermEnumCounts
   */
  public NamedList getFacetFieldCounts()
          throws IOException, ParseException {

    NamedList res = new SimpleOrderedMap();
    String[] facetFs = params.getParams(FacetParams.FACET_FIELD);
    if (null != facetFs) {
      for (String f : facetFs) {
        parseParams(FacetParams.FACET_FIELD, f);
        String termList = localParams == null ? null : localParams.get(CommonParams.TERMS);
        if (termList != null) {
          res.add(key, getListedTermCounts(facetValue, termList));
        } else {
          res.add(key, getTermCounts(facetValue));
        }
      }
    }
    return res;
  }


  private NamedList getListedTermCounts(String field, String termList) throws IOException {
    FieldType ft = searcher.getSchema().getFieldType(field);
    List<String> terms = StrUtils.splitSmart(termList, ",", true);
    NamedList res = new NamedList();
    Term t = new Term(field);
    for (String term : terms) {
      String internal = ft.toInternal(term);
      int count = searcher.numDocs(new TermQuery(t.createTerm(internal)), base);
      res.add(term, count);
    }
    return res;    
  }


  /**
   * Returns a count of the documents in the set which do not have any 
   * terms for for the specified field.
   *
   * @see FacetParams#FACET_MISSING
   */
  public static int getFieldMissingCount(SolrIndexSearcher searcher, DocSet docs, String fieldName)
    throws IOException {

    DocSet hasVal = searcher.getDocSet
      (new TermRangeQuery(fieldName, null, null, false, false));
    return docs.andNotSize(hasVal);
  }


  // first element of the fieldcache is null, so we need this comparator.
  private static final Comparator nullStrComparator = new Comparator() {
        public int compare(Object o1, Object o2) {
          if (o1==null) return (o2==null) ? 0 : -1;
          else if (o2==null) return 1;
          return ((String)o1).compareTo((String)o2);
        }
      }; 

  /**
   * Use the Lucene FieldCache to get counts for each unique field value in <code>docs</code>.
   * The field must have at most one indexed token per document.
   */
  public static NamedList getFieldCacheCounts(SolrIndexSearcher searcher, DocSet docs, String fieldName, int offset, int limit, int mincount, boolean missing, String sort, String prefix) throws IOException {
    // TODO: If the number of terms is high compared to docs.size(), and zeros==false,
    //  we should use an alternate strategy to avoid
    //  1) creating another huge int[] for the counts
    //  2) looping over that huge int[] looking for the rare non-zeros.
    //
    // Yet another variation: if docs.size() is small and termvectors are stored,
    // then use them instead of the FieldCache.
    //

    // TODO: this function is too big and could use some refactoring, but
    // we also need a facet cache, and refactoring of SimpleFacets instead of
    // trying to pass all the various params around.

    FieldType ft = searcher.getSchema().getFieldType(fieldName);
    NamedList res = new NamedList();

    FieldCache.StringIndex si = FieldCache.DEFAULT.getStringIndex(searcher.getReader(), fieldName);
    final String[] terms = si.lookup;
    final int[] termNum = si.order;

    if (prefix!=null && prefix.length()==0) prefix=null;

    int startTermIndex, endTermIndex;
    if (prefix!=null) {
      startTermIndex = Arrays.binarySearch(terms,prefix,nullStrComparator);
      if (startTermIndex<0) startTermIndex=-startTermIndex-1;
      // find the end term.  \uffff isn't a legal unicode char, but only compareTo
      // is used, so it should be fine, and is guaranteed to be bigger than legal chars.
      endTermIndex = Arrays.binarySearch(terms,prefix+"\uffff\uffff\uffff\uffff",nullStrComparator);
      endTermIndex = -endTermIndex-1;
    } else {
      startTermIndex=1;
      endTermIndex=terms.length;
    }

    final int nTerms=endTermIndex-startTermIndex;

    if (nTerms>0 && docs.size() >= mincount) {

      // count collection array only needs to be as big as the number of terms we are
      // going to collect counts for.
      final int[] counts = new int[nTerms];

      DocIterator iter = docs.iterator();
      while (iter.hasNext()) {
        int term = termNum[iter.nextDoc()];
        int arrIdx = term-startTermIndex;
        if (arrIdx>=0 && arrIdx<nTerms) counts[arrIdx]++;
      }

      // IDEA: we could also maintain a count of "other"... everything that fell outside
      // of the top 'N'

      int off=offset;
      int lim=limit>=0 ? limit : Integer.MAX_VALUE;

      if (sort.equals(FacetParams.FACET_SORT_COUNT) || sort.equals(FacetParams.FACET_SORT_COUNT_LEGACY)) {
        int maxsize = limit>0 ? offset+limit : Integer.MAX_VALUE-1;
        maxsize = Math.min(maxsize, nTerms);
        final BoundedTreeSet<CountPair<String,Integer>> queue = new BoundedTreeSet<CountPair<String,Integer>>(maxsize);
        int min=mincount-1;  // the smallest value in the top 'N' values
        for (int i=0; i<nTerms; i++) {
          int c = counts[i];
          if (c>min) {
            // NOTE: we use c>min rather than c>=min as an optimization because we are going in
            // index order, so we already know that the keys are ordered.  This can be very
            // important if a lot of the counts are repeated (like zero counts would be).
            queue.add(new CountPair<String,Integer>(terms[startTermIndex+i], c));
            if (queue.size()>=maxsize) min=queue.last().val;
          }
        }
        // now select the right page from the results
        for (CountPair<String,Integer> p : queue) {
          if (--off>=0) continue;
          if (--lim<0) break;
          res.add(ft.indexedToReadable(p.key), p.val);
        }
      } else {
        // add results in index order
        int i=0;
        if (mincount<=0) {
          // if mincount<=0, then we won't discard any terms and we know exactly
          // where to start.
          i=off;
          off=0;
        }

        for (; i<nTerms; i++) {          
          int c = counts[i];
          if (c<mincount || --off>=0) continue;
          if (--lim<0) break;
          res.add(ft.indexedToReadable(terms[startTermIndex+i]), c);
        }
      }
    }

    if (missing) {
      res.add(null, getFieldMissingCount(searcher,docs,fieldName));
    }
    
    return res;
  }


  /**
   * Returns a list of terms in the specified field along with the 
   * corresponding count of documents in the set that match that constraint.
   * This method uses the FilterCache to get the intersection count between <code>docs</code>
   * and the DocSet for each term in the filter.
   *
   * @see FacetParams#FACET_LIMIT
   * @see FacetParams#FACET_ZEROS
   * @see FacetParams#FACET_MISSING
   */
  public NamedList getFacetTermEnumCounts(SolrIndexSearcher searcher, DocSet docs, String field, int offset, int limit, int mincount, boolean missing, String sort, String prefix)
    throws IOException {

    /* :TODO: potential optimization...
    * cache the Terms with the highest docFreq and try them first
    * don't enum if we get our max from them
    */

    // Minimum term docFreq in order to use the filterCache for that term.
    int minDfFilterCache = params.getFieldInt(field, FacetParams.FACET_ENUM_CACHE_MINDF, 0);

    IndexSchema schema = searcher.getSchema();
    IndexReader r = searcher.getReader();
    FieldType ft = schema.getFieldType(field);

    final int maxsize = limit>=0 ? offset+limit : Integer.MAX_VALUE-1;    
    final BoundedTreeSet<CountPair<String,Integer>> queue = (sort.equals("count") || sort.equals("true")) ? new BoundedTreeSet<CountPair<String,Integer>>(maxsize) : null;
    final NamedList res = new NamedList();

    int min=mincount-1;  // the smallest value in the top 'N' values    
    int off=offset;
    int lim=limit>=0 ? limit : Integer.MAX_VALUE;

    String startTerm = prefix==null ? "" : ft.toInternal(prefix);
    TermEnum te = r.terms(new Term(field,startTerm));
    TermDocs td = r.termDocs();

    if (docs.size() >= mincount) { 
    do {
      Term t = te.term();

      if (null == t || ! t.field().equals(field))
        break;

      if (prefix!=null && !t.text().startsWith(prefix)) break;

      int df = te.docFreq();

      // If we are sorting, we can use df>min (rather than >=) since we
      // are going in index order.  For certain term distributions this can
      // make a large difference (for example, many terms with df=1).
      if (df>0 && df>min) {
        int c;

        if (df >= minDfFilterCache) {
          // use the filter cache
          c = searcher.numDocs(new TermQuery(t), docs);
        } else {
          // iterate over TermDocs to calculate the intersection
          td.seek(te);
          c=0;
          while (td.next()) {
            if (docs.exists(td.doc())) c++;
          }
        }

        if (sort.equals("count") || sort.equals("true")) {
          if (c>min) {
            queue.add(new CountPair<String,Integer>(t.text(), c));
            if (queue.size()>=maxsize) min=queue.last().val;
          }
        } else {
          if (c >= mincount && --off<0) {
            if (--lim<0) break;
            res.add(ft.indexedToReadable(t.text()), c);
          }
        }
      }
    } while (te.next());
    }

    if (sort.equals("count") || sort.equals("true")) {
      for (CountPair<String,Integer> p : queue) {
        if (--off>=0) continue;
        if (--lim<0) break;
        res.add(ft.indexedToReadable(p.key), p.val);
      }
    }

    if (missing) {
      res.add(null, getFieldMissingCount(searcher,docs,field));
    }

    te.close();
    td.close();    

    return res;
  }

  /**
   * Returns a list of value constraints and the associated facet counts 
   * for each facet date field, range, and interval specified in the
   * SolrParams
   *
   * @see FacetParams#FACET_DATE
   */
  public NamedList getFacetDateCounts()
          throws IOException, ParseException {

    final SolrParams required = new RequiredSolrParams(params);
    final NamedList resOuter = new SimpleOrderedMap();
    final String[] fields = params.getParams(FacetParams.FACET_DATE);
    final Date NOW = new Date();
    
    if (null == fields || 0 == fields.length) return resOuter;
    
    final IndexSchema schema = searcher.getSchema();
    for (String f : fields) {
      parseParams(FacetParams.FACET_DATE, f);
      f = facetValue;


      final NamedList resInner = new SimpleOrderedMap();
      resOuter.add(key, resInner);
      final FieldType trash = schema.getFieldType(f);
      if (! (trash instanceof DateField)) {
        throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
           "Can not date facet on a field which is not a DateField: " + f);
      }
      final DateField ft = (DateField) trash;
      final String startS
        = required.getFieldParam(f,FacetParams.FACET_DATE_START);
      final Date start;
      try {
        start = ft.parseMath(NOW, startS);
      } catch (SolrException e) {
        throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
           "date facet 'start' is not a valid Date string: " + startS, e);
      }
      final String endS
        = required.getFieldParam(f,FacetParams.FACET_DATE_END);
      Date end; // not final, hardend may change this
      try {
        end = ft.parseMath(NOW, endS);
      } catch (SolrException e) {
        throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
           "date facet 'end' is not a valid Date string: " + endS, e);
      }
          
      if (end.before(start)) {
        throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
           "date facet 'end' comes before 'start': "+endS+" < "+startS);
      }

      final String gap = required.getFieldParam(f,FacetParams.FACET_DATE_GAP);
      final DateMathParser dmp = new DateMathParser(ft.UTC, Locale.US);
      dmp.setNow(NOW);
      
      try {
        
        Date low = start;
        while (low.before(end)) {
          dmp.setNow(low);
          final String lowI = ft.toInternal(low);
          final String label = ft.indexedToReadable(lowI);
          Date high = dmp.parseMath(gap);
          if (end.before(high)) {
            if (params.getFieldBool(f,FacetParams.FACET_DATE_HARD_END,false)) {
              high = end;
            } else {
              end = high;
            }
          }
          if (high.before(low)) {
            throw new SolrException
              (SolrException.ErrorCode.BAD_REQUEST,
               "date facet infinite loop (is gap negative?)");
          }
          final String highI = ft.toInternal(high);
          resInner.add(label, rangeCount(f,lowI,highI,true,true));
          low = high;
        }
      } catch (java.text.ParseException e) {
        throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
           "date facet 'gap' is not a valid Date Math string: " + gap, e);
      }
      
      // explicitly return the gap and end so all the counts are meaningful
      resInner.add("gap", gap);
      resInner.add("end", end);

      final String[] othersP =
        params.getFieldParams(f,FacetParams.FACET_DATE_OTHER);
      if (null != othersP && 0 < othersP.length ) {
        Set<FacetDateOther> others = EnumSet.noneOf(FacetDateOther.class);

        for (final String o : othersP) {
          others.add(FacetDateOther.get(o));
        }

        // no matter what other values are listed, we don't do
        // anything if "none" is specified.
        if (! others.contains(FacetDateOther.NONE) ) {
          final String startI = ft.toInternal(start);
          final String endI = ft.toInternal(end);
          
          boolean all = others.contains(FacetDateOther.ALL);
        
          if (all || others.contains(FacetDateOther.BEFORE)) {
            resInner.add(FacetDateOther.BEFORE.toString(),
                         rangeCount(f,null,startI,false,false));
          }
          if (all || others.contains(FacetDateOther.AFTER)) {
            resInner.add(FacetDateOther.AFTER.toString(),
                         rangeCount(f,endI,null,false,false));
          }
          if (all || others.contains(FacetDateOther.BETWEEN)) {
            resInner.add(FacetDateOther.BETWEEN.toString(),
                         rangeCount(f,startI,endI,true,true));
          }
        }
      }
    }
    
    return resOuter;
  }

  /**
   * Macro for getting the numDocs of a TermRangeQuery over docs
   * @see SolrIndexSearcher#numDocs
   * @see TermRangeQuery
   */
  protected int rangeCount(String field, String low, String high,
                           boolean iLow, boolean iHigh) throws IOException {
    return searcher.numDocs(new TermRangeQuery(field,low,high,iLow,iHigh),
                            base);
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


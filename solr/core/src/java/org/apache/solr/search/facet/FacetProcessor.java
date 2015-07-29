package org.apache.solr.search.facet;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;

public class FacetProcessor<FacetRequestT extends FacetRequest>  {
  protected SimpleOrderedMap<Object> response;
  protected FacetContext fcontext;
  protected FacetRequestT freq;

  LinkedHashMap<String,SlotAcc> accMap;
  protected SlotAcc[] accs;
  protected CountSlotAcc countAcc;

  FacetProcessor(FacetContext fcontext, FacetRequestT freq) {
    this.fcontext = fcontext;
    this.freq = freq;
  }

  public void process() throws IOException {
    handleDomainChanges();
  }

  protected void handleDomainChanges() throws IOException {
    if (freq.domain == null) return;
    handleFilterExclusions();
    handleBlockJoin();
  }

  private void handleBlockJoin() throws IOException {
    if (!(freq.domain.toChildren || freq.domain.toParent)) return;

    // TODO: avoid query parsing per-bucket somehow...
    String parentStr = freq.domain.parents;
    Query parentQuery;
    try {
      QParser parser = QParser.getParser(parentStr, null, fcontext.req);
      parentQuery = parser.getQuery();
    } catch (SyntaxError err) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error parsing block join parent specification: " + parentStr);
    }

    BitDocSet parents = fcontext.searcher.getDocSetBits(parentQuery);
    DocSet input = fcontext.base;
    DocSet result;

    if (freq.domain.toChildren) {
      DocSet filt = fcontext.searcher.getDocSetBits( new MatchAllDocsQuery() );
      result = BlockJoin.toChildren(input, parents, filt, fcontext.qcontext);
    } else {
      result = BlockJoin.toParents(input, parents, fcontext.qcontext);
    }

    fcontext.base = result;
  }

  private void handleFilterExclusions() throws IOException {
    List<String> excludeTags = freq.domain.excludeTags;

    if (excludeTags == null || excludeTags.size() == 0) {
      return;
    }

    // TODO: somehow remove responsebuilder dependency
    ResponseBuilder rb = SolrRequestInfo.getRequestInfo().getResponseBuilder();
    Map tagMap = (Map) rb.req.getContext().get("tags");
    if (tagMap == null) {
      // no filters were tagged
      return;
    }

    IdentityHashMap<Query,Boolean> excludeSet = new IdentityHashMap<>();
    for (String excludeTag : excludeTags) {
      Object olst = tagMap.get(excludeTag);
      // tagMap has entries of List<String,List<QParser>>, but subject to change in the future
      if (!(olst instanceof Collection)) continue;
      for (Object o : (Collection<?>)olst) {
        if (!(o instanceof QParser)) continue;
        QParser qp = (QParser)o;
        try {
          excludeSet.put(qp.getQuery(), Boolean.TRUE);
        } catch (SyntaxError syntaxError) {
          // This should not happen since we should only be retrieving a previously parsed query
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, syntaxError);
        }
      }
    }
    if (excludeSet.size() == 0) return;

    List<Query> qlist = new ArrayList<>();

    // add the base query
    if (!excludeSet.containsKey(rb.getQuery())) {
      qlist.add(rb.getQuery());
    }

    // add the filters
    if (rb.getFilters() != null) {
      for (Query q : rb.getFilters()) {
        if (!excludeSet.containsKey(q)) {
          qlist.add(q);
        }
      }
    }

    // now walk back up the context tree
    // TODO: we lose parent exclusions...
    for (FacetContext curr = fcontext; curr != null; curr = curr.parent) {
      if (curr.filter != null) {
        qlist.add( curr.filter );
      }
    }

    // recompute the base domain
    fcontext.base = fcontext.searcher.getDocSet(qlist);
  }


  public Object getResponse() {
    return null;
  }


  protected void createAccs(int docCount, int slotCount) throws IOException {
    accMap = new LinkedHashMap<>();

    // allow a custom count acc to be used
    if (countAcc == null) {
      countAcc = new CountSlotArrAcc(fcontext, slotCount);
      countAcc.key = "count";
    }

    for (Map.Entry<String,AggValueSource> entry : freq.getFacetStats().entrySet()) {
      SlotAcc acc = entry.getValue().createSlotAcc(fcontext, docCount, slotCount);
      acc.key = entry.getKey();
      accMap.put(acc.key, acc);
    }


    accs = new SlotAcc[accMap.size()];
    int i=0;
    for (SlotAcc acc : accMap.values()) {
      accs[i++] = acc;
    }
  }


  protected void resetStats() {
    countAcc.reset();
    for (SlotAcc acc : accs) {
      acc.reset();
    }
  }

  protected void processStats(SimpleOrderedMap<Object> bucket, DocSet docs, int docCount) throws IOException {
    if (docCount == 0 && !freq.processEmpty || freq.getFacetStats().size() == 0) {
      bucket.add("count", docCount);
      return;
    }
    createAccs(docCount, 1);
    int collected = collect(docs, 0);
    countAcc.incrementCount(0, collected);
    assert collected == docCount;
    addStats(bucket, 0);
  }


  protected void processSubs(SimpleOrderedMap<Object> response, Query filter, DocSet domain) throws IOException {

    // TODO: what if a zero bucket has a sub-facet with an exclusion that would yield results?
    // should we check for domain-altering exclusions, or even ask the sub-facet for
    // it's domain and then only skip it if it's 0?

    if (domain == null || domain.size() == 0 && !freq.processEmpty) {
      return;
    }

    for (Map.Entry<String,FacetRequest> sub : freq.getSubFacets().entrySet()) {
      // make a new context for each sub-facet since they can change the domain
      FacetContext subContext = fcontext.sub(filter, domain);
      FacetProcessor subProcessor = sub.getValue().createFacetProcessor(subContext);
      subProcessor.process();
      response.add( sub.getKey(), subProcessor.getResponse() );
    }
  }

  int collect(DocSet docs, int slot) throws IOException {
    int count = 0;
    SolrIndexSearcher searcher = fcontext.searcher;

    final List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    final Iterator<LeafReaderContext> ctxIt = leaves.iterator();
    LeafReaderContext ctx = null;
    int segBase = 0;
    int segMax;
    int adjustedMax = 0;
    for (DocIterator docsIt = docs.iterator(); docsIt.hasNext(); ) {
      final int doc = docsIt.nextDoc();
      if (doc >= adjustedMax) {
        do {
          ctx = ctxIt.next();
          if (ctx == null) {
            // should be impossible
            throw new RuntimeException("INTERNAL FACET ERROR");
          }
          segBase = ctx.docBase;
          segMax = ctx.reader().maxDoc();
          adjustedMax = segBase + segMax;
        } while (doc >= adjustedMax);
        assert doc >= ctx.docBase;
        setNextReader(ctx);
      }
      count++;
      collect(doc - segBase, slot);  // per-seg collectors
    }
    return count;
  }

  void collect(int segDoc, int slot) throws IOException {
    if (accs != null) {
      for (SlotAcc acc : accs) {
        acc.collect(segDoc, slot);
      }
    }
  }

  void setNextReader(LeafReaderContext ctx) throws IOException {
    // countAcc.setNextReader is a no-op
    for (SlotAcc acc : accs) {
      acc.setNextReader(ctx);
    }
  }


  void addStats(SimpleOrderedMap<Object> target, int slotNum) throws IOException {
    int count = countAcc.getCount(slotNum);
    target.add("count", count);
    if (count > 0 || freq.processEmpty) {
      for (SlotAcc acc : accs) {
        acc.setValues(target, slotNum);
      }
    }
  }


  public void fillBucket(SimpleOrderedMap<Object> bucket, Query q, DocSet result) throws IOException {
    boolean needDocSet = freq.getFacetStats().size() > 0 || freq.getSubFacets().size() > 0;

    // TODO: always collect counts or not???

    int count;

    if (result != null) {
      count = result.size();
    } else if (needDocSet) {
      if (q == null) {
        result = fcontext.base;
        // result.incref(); // OFF-HEAP
      } else {
        result = fcontext.searcher.getDocSet(q, fcontext.base);
      }
      count = result.size();
    } else {
      if (q == null) {
        count = fcontext.base.size();
      } else {
        count = fcontext.searcher.numDocs(q, fcontext.base);
      }
    }

    try {
      processStats(bucket, result, (int) count);
      processSubs(bucket, q, result);
    } finally {
      if (result != null) {
        // result.decref(); // OFF-HEAP
        result = null;
      }
    }
  }

  public static DocSet getFieldMissing(SolrIndexSearcher searcher, DocSet docs, String fieldName) throws IOException {
    SchemaField sf = searcher.getSchema().getField(fieldName);
    DocSet hasVal = searcher.getDocSet(sf.getType().getRangeQuery(null, sf, null, null, false, false));
    DocSet answer = docs.andNot(hasVal);
    // hasVal.decref(); // OFF-HEAP
    return answer;
  }

  public static Query getFieldMissingQuery(SolrIndexSearcher searcher, String fieldName) throws IOException {
    SchemaField sf = searcher.getSchema().getField(fieldName);
    Query hasVal = sf.getType().getRangeQuery(null, sf, null, null, false, false);
    BooleanQuery.Builder noVal = new BooleanQuery.Builder();
    noVal.add(hasVal, BooleanClause.Occur.MUST_NOT);
    return noVal.build();
  }

}

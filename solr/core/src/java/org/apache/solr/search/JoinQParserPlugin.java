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
package org.apache.solr.search;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiDocsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.StringHelper;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.TrieField;
import org.apache.solr.util.RefCounted;


public class JoinQParserPlugin extends QParserPlugin {
  public static final String NAME = "join";

  @Override
  public void init(NamedList args) {
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        String fromField = getParam("from");
        String fromIndex = getParam("fromIndex");
        String toField = getParam("to");
        String v = localParams.get("v");
        Query fromQuery;
        long fromCoreOpenTime = 0;

        if (fromIndex != null && !fromIndex.equals(req.getCore().getCoreDescriptor().getName()) ) {
          CoreContainer container = req.getCore().getCoreDescriptor().getCoreContainer();

          final SolrCore fromCore = container.getCore(fromIndex);
          RefCounted<SolrIndexSearcher> fromHolder = null;

          if (fromCore == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + fromIndex);
          }

          LocalSolrQueryRequest otherReq = new LocalSolrQueryRequest(fromCore, params);
          try {
            QParser parser = QParser.getParser(v, "lucene", otherReq);
            fromQuery = parser.getQuery();
            fromHolder = fromCore.getRegisteredSearcher();
            if (fromHolder != null) fromCoreOpenTime = fromHolder.get().getOpenTime();
          } finally {
            otherReq.close();
            fromCore.close();
            if (fromHolder != null) fromHolder.decref();
          }
        } else {
          QParser fromQueryParser = subQuery(v, null);
          fromQuery = fromQueryParser.getQuery();
        }

        JoinQuery jq = new JoinQuery(fromField, toField, fromIndex, fromQuery);
        jq.fromCoreOpenTime = fromCoreOpenTime;
        return jq;
      }
    };
  }
}


class JoinQuery extends Query {
  String fromField;
  String toField;
  String fromIndex;
  Query q;
  long fromCoreOpenTime;

  public JoinQuery(String fromField, String toField, String fromIndex, Query subQuery) {
    this.fromField = fromField;
    this.toField = toField;
    this.fromIndex = fromIndex;
    this.q = subQuery;
  }

  public Query getQuery() { return q; }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    // don't rewrite the subQuery
    return this;
  }

  @Override
  public void extractTerms(Set terms) {
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new JoinQueryWeight((SolrIndexSearcher)searcher);
  }

  private class JoinQueryWeight extends Weight {
    SolrIndexSearcher fromSearcher;
    RefCounted<SolrIndexSearcher> fromRef;
    SolrIndexSearcher toSearcher;
    private Similarity similarity;
    private float queryNorm;
    private float queryWeight;
    ResponseBuilder rb;

    public JoinQueryWeight(SolrIndexSearcher searcher) {
      this.fromSearcher = searcher;
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
      if (info != null) {
        rb = info.getResponseBuilder();
      }

      if (fromIndex == null) {
        this.fromSearcher = searcher;
      } else {
        if (info == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join must have SolrRequestInfo");
        }

        CoreContainer container = searcher.getCore().getCoreDescriptor().getCoreContainer();
        final SolrCore fromCore = container.getCore(fromIndex);

        if (fromCore == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + fromIndex);
        }

        if (info.getReq().getCore() == fromCore) {
          // if this is the same core, use the searcher passed in... otherwise we could be warming and
          // get an older searcher from the core.
          fromSearcher = searcher;
        } else {
          // This could block if there is a static warming query with a join in it, and if useColdSearcher is true.
          // Deadlock could result if two cores both had useColdSearcher and had joins that used eachother.
          // This would be very predictable though (should happen every time if misconfigured)
          fromRef = fromCore.getSearcher(false, true, null);

          // be careful not to do anything with this searcher that requires the thread local
          // SolrRequestInfo in a manner that requires the core in the request to match
          fromSearcher = fromRef.get();
        }

        if (fromRef != null) {
          final RefCounted<SolrIndexSearcher> ref = fromRef;
          info.addCloseHook(new Closeable() {
            @Override
            public void close() {
              ref.decref();
            }
          });
        }

        info.addCloseHook(new Closeable() {
          @Override
          public void close() {
            fromCore.close();
          }
        });

      }
      this.toSearcher = searcher;
    }

    @Override
    public Query getQuery() {
      return JoinQuery.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      queryWeight = getBoost();
      return queryWeight * queryWeight;
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      this.queryNorm = norm * topLevelBoost;
      queryWeight *= this.queryNorm;
    }

    DocSet resultSet;
    Filter filter;



    @Override
    public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
      if (filter == null) {
        boolean debug = rb != null && rb.isDebug();
        long start = debug ? System.currentTimeMillis() : 0;
        resultSet = getDocSet();
        long end = debug ? System.currentTimeMillis() : 0;

        if (debug) {
          SimpleOrderedMap<Object> dbg = new SimpleOrderedMap<>();
          dbg.add("time", (end-start));
          dbg.add("fromSetSize", fromSetSize);  // the input
          dbg.add("toSetSize", resultSet.size());    // the output

          dbg.add("fromTermCount", fromTermCount);
          dbg.add("fromTermTotalDf", fromTermTotalDf);
          dbg.add("fromTermDirectCount", fromTermDirectCount);
          dbg.add("fromTermHits", fromTermHits);
          dbg.add("fromTermHitsTotalDf", fromTermHitsTotalDf);
          dbg.add("toTermHits", toTermHits);
          dbg.add("toTermHitsTotalDf", toTermHitsTotalDf);
          dbg.add("toTermDirectCount", toTermDirectCount);
          dbg.add("smallSetsDeferred", smallSetsDeferred);
          dbg.add("toSetDocsAdded", resultListDocs);

          // TODO: perhaps synchronize  addDebug in the future...
          rb.addDebug(dbg, "join", JoinQuery.this.toString());
        }

        filter = resultSet.getTopFilter();
      }

      // Although this set only includes live docs, other filters can be pushed down to queries.
      DocIdSet readerSet = filter.getDocIdSet(context, acceptDocs);
      return new JoinScorer(this, readerSet == null ? DocIdSetIterator.empty() : readerSet.iterator(), getBoost());
    }


    int fromSetSize;          // number of docs in the fromSet (that match the from query)
    long resultListDocs;      // total number of docs collected
    int fromTermCount;
    long fromTermTotalDf;
    int fromTermDirectCount;  // number of fromTerms that were too small to use the filter cache
    int fromTermHits;         // number of fromTerms that intersected the from query
    long fromTermHitsTotalDf; // sum of the df of the matching terms
    int toTermHits;           // num if intersecting from terms that match a term in the to field
    long toTermHitsTotalDf;   // sum of the df for the toTermHits
    int toTermDirectCount;    // number of toTerms that we set directly on a bitset rather than doing set intersections
    int smallSetsDeferred;    // number of small sets collected to be used later to intersect w/ bitset or create another small set


    public DocSet getDocSet() throws IOException {
      FixedBitSet resultBits = null;

      // minimum docFreq to use the cache
      int minDocFreqFrom = Math.max(5, fromSearcher.maxDoc() >> 13);
      int minDocFreqTo = Math.max(5, toSearcher.maxDoc() >> 13);

      // use a smaller size than normal since we will need to sort and dedup the results
      int maxSortedIntSize = Math.max(10, toSearcher.maxDoc() >> 10);

      DocSet fromSet = fromSearcher.getDocSet(q);
      fromSetSize = fromSet.size();

      List<DocSet> resultList = new ArrayList<>(10);

      // make sure we have a set that is fast for random access, if we will use it for that
      DocSet fastForRandomSet = fromSet;
      if (minDocFreqFrom>0 && fromSet instanceof SortedIntDocSet) {
        SortedIntDocSet sset = (SortedIntDocSet)fromSet;
        fastForRandomSet = new HashDocSet(sset.getDocs(), 0, sset.size());
      }

      Fields fromFields = fromSearcher.getLeafReader().fields();
      Fields toFields = fromSearcher==toSearcher ? fromFields : toSearcher.getLeafReader().fields();
      if (fromFields == null) return DocSet.EMPTY;
      Terms terms = fromFields.terms(fromField);
      Terms toTerms = toFields.terms(toField);
      if (terms == null || toTerms==null) return DocSet.EMPTY;
      String prefixStr = TrieField.getMainValuePrefix(fromSearcher.getSchema().getFieldType(fromField));
      BytesRef prefix = prefixStr == null ? null : new BytesRef(prefixStr);

      BytesRef term = null;
      TermsEnum  termsEnum = terms.iterator(null);
      TermsEnum  toTermsEnum = toTerms.iterator(null);
      SolrIndexSearcher.DocsEnumState fromDeState = null;
      SolrIndexSearcher.DocsEnumState toDeState = null;

      if (prefix == null) {
        term = termsEnum.next();
      } else {
        if (termsEnum.seekCeil(prefix) != TermsEnum.SeekStatus.END) {
          term = termsEnum.term();
        }
      }

      Bits fromLiveDocs = fromSearcher.getLeafReader().getLiveDocs();
      Bits toLiveDocs = fromSearcher == toSearcher ? fromLiveDocs : toSearcher.getLeafReader().getLiveDocs();

      fromDeState = new SolrIndexSearcher.DocsEnumState();
      fromDeState.fieldName = fromField;
      fromDeState.liveDocs = fromLiveDocs;
      fromDeState.termsEnum = termsEnum;
      fromDeState.docsEnum = null;
      fromDeState.minSetSizeCached = minDocFreqFrom;

      toDeState = new SolrIndexSearcher.DocsEnumState();
      toDeState.fieldName = toField;
      toDeState.liveDocs = toLiveDocs;
      toDeState.termsEnum = toTermsEnum;
      toDeState.docsEnum = null;
      toDeState.minSetSizeCached = minDocFreqTo;

      while (term != null) {
        if (prefix != null && !StringHelper.startsWith(term, prefix))
          break;

        fromTermCount++;

        boolean intersects = false;
        int freq = termsEnum.docFreq();
        fromTermTotalDf++;

        if (freq < minDocFreqFrom) {
          fromTermDirectCount++;
          // OK to skip liveDocs, since we check for intersection with docs matching query
          fromDeState.docsEnum = fromDeState.termsEnum.docs(null, fromDeState.docsEnum, DocsEnum.FLAG_NONE);
          DocsEnum docsEnum = fromDeState.docsEnum;

          if (docsEnum instanceof MultiDocsEnum) {
            MultiDocsEnum.EnumWithSlice[] subs = ((MultiDocsEnum)docsEnum).getSubs();
            int numSubs = ((MultiDocsEnum)docsEnum).getNumSubs();
            outer: for (int subindex = 0; subindex<numSubs; subindex++) {
              MultiDocsEnum.EnumWithSlice sub = subs[subindex];
              if (sub.docsEnum == null) continue;
              int base = sub.slice.start;
              int docid;
              while ((docid = sub.docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (fastForRandomSet.exists(docid+base)) {
                  intersects = true;
                  break outer;
                }
              }
            }
          } else {
            int docid;
            while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              if (fastForRandomSet.exists(docid)) {
                intersects = true;
                break;
              }
            }
          }
        } else {
          // use the filter cache
          DocSet fromTermSet = fromSearcher.getDocSet(fromDeState);
          intersects = fromSet.intersects(fromTermSet);
        }

        if (intersects) {
          fromTermHits++;
          fromTermHitsTotalDf++;
          TermsEnum.SeekStatus status = toTermsEnum.seekCeil(term);
          if (status == TermsEnum.SeekStatus.END) break;
          if (status == TermsEnum.SeekStatus.FOUND) {
            toTermHits++;
            int df = toTermsEnum.docFreq();
            toTermHitsTotalDf += df;
            if (resultBits==null && df + resultListDocs > maxSortedIntSize && resultList.size() > 0) {
              resultBits = new FixedBitSet(toSearcher.maxDoc());
            }

            // if we don't have a bitset yet, or if the resulting set will be too large
            // use the filterCache to get a DocSet
            if (toTermsEnum.docFreq() >= minDocFreqTo || resultBits == null) {
              // use filter cache
              DocSet toTermSet = toSearcher.getDocSet(toDeState);
              resultListDocs += toTermSet.size();
              if (resultBits != null) {
                toTermSet.addAllTo(new BitDocSet(resultBits));
              } else {
                if (toTermSet instanceof BitDocSet) {
                  resultBits = ((BitDocSet)toTermSet).bits.clone();
                } else {
                  resultList.add(toTermSet);
                }
              }
            } else {
              toTermDirectCount++;

              // need to use liveDocs here so we don't map to any deleted ones
              toDeState.docsEnum = toDeState.termsEnum.docs(toDeState.liveDocs, toDeState.docsEnum, DocsEnum.FLAG_NONE);
              DocsEnum docsEnum = toDeState.docsEnum;              

              if (docsEnum instanceof MultiDocsEnum) {
                MultiDocsEnum.EnumWithSlice[] subs = ((MultiDocsEnum)docsEnum).getSubs();
                int numSubs = ((MultiDocsEnum)docsEnum).getNumSubs();
                for (int subindex = 0; subindex<numSubs; subindex++) {
                  MultiDocsEnum.EnumWithSlice sub = subs[subindex];
                  if (sub.docsEnum == null) continue;
                  int base = sub.slice.start;
                  int docid;
                  while ((docid = sub.docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    resultListDocs++;
                    resultBits.set(docid + base);
                  }
                }
              } else {
                int docid;
                while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  resultListDocs++;
                  resultBits.set(docid);
                }
              }
            }

          }
        }

        term = termsEnum.next();
      }

      smallSetsDeferred = resultList.size();

      if (resultBits != null) {
        BitDocSet bitSet = new BitDocSet(resultBits);
        for (DocSet set : resultList) {
          set.addAllTo(bitSet);
        }
        return bitSet;
      }

      if (resultList.size()==0) {
        return DocSet.EMPTY;
      }

      if (resultList.size() == 1) {
        return resultList.get(0);
      }

      int sz = 0;

      for (DocSet set : resultList)
        sz += set.size();

      int[] docs = new int[sz];
      int pos = 0;
      for (DocSet set : resultList) {
        System.arraycopy(((SortedIntDocSet)set).getDocs(), 0, docs, pos, set.size());
        pos += set.size();
      }
      Arrays.sort(docs);
      int[] dedup = new int[sz];
      pos = 0;
      int last = -1;
      for (int doc : docs) {
        if (doc != last)
          dedup[pos++] = doc;
        last = doc;
      }

      if (pos != dedup.length) {
        dedup = Arrays.copyOf(dedup, pos);
      }

      return new SortedIntDocSet(dedup, dedup.length);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Scorer scorer = scorer(context, context.reader().getLiveDocs());
      boolean exists = scorer.advance(doc) == doc;

      ComplexExplanation result = new ComplexExplanation();

      if (exists) {
        result.setDescription(this.toString()
        + " , product of:");
        result.setValue(queryWeight);
        result.setMatch(Boolean.TRUE);
        result.addDetail(new Explanation(getBoost(), "boost"));
        result.addDetail(new Explanation(queryNorm,"queryNorm"));
      } else {
        result.setDescription(this.toString()
        + " doesn't match id " + doc);
        result.setValue(0);
        result.setMatch(Boolean.FALSE);
      }
      return result;
    }
  }


  protected static class JoinScorer extends Scorer {
    final DocIdSetIterator iter;
    final float score;
    int doc = -1;

    public JoinScorer(Weight w, DocIdSetIterator iter, float score) throws IOException {
      super(w);
      this.score = score;
      this.iter = iter==null ? DocIdSetIterator.empty() : iter;
    }

    @Override
    public int nextDoc() throws IOException {
      return iter.nextDoc();
    }

    @Override
    public int docID() {
      return iter.docID();
    }

    @Override
    public float score() throws IOException {
      return score;
    }
    
    @Override
    public int freq() throws IOException {
      return 1;
    }

    @Override
    public int advance(int target) throws IOException {
      return iter.advance(target);
    }

    @Override
    public long cost() {
      return iter.cost();
    }
  }


  @Override
  public String toString(String field) {
    return "{!join from="+fromField+" to="+toField
        + (fromIndex != null ? " fromIndex="+fromIndex : "")
        +"}"+q.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) return false;
    JoinQuery other = (JoinQuery)o;
    return this.fromField.equals(other.fromField)
           && this.toField.equals(other.toField)
           && this.getBoost() == other.getBoost()
           && this.q.equals(other.q)
           && (this.fromIndex == other.fromIndex || this.fromIndex != null && this.fromIndex.equals(other.fromIndex))
           && this.fromCoreOpenTime == other.fromCoreOpenTime
        ;
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = h * 31 + q.hashCode();
    h = h * 31 + (int)fromCoreOpenTime;
    h = h * 31 + fromField.hashCode();
    h = h * 31 + toField.hashCode();
    return h;
  }

}

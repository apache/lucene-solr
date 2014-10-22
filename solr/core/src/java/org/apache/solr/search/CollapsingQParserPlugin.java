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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FixedBitSet.FixedBitSetIterator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.QueryElevationComponent;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.TrieFloatField;
import org.apache.solr.schema.TrieIntField;
import org.apache.solr.schema.TrieLongField;

import com.carrotsearch.hppc.FloatArrayList;
import com.carrotsearch.hppc.IntIntOpenHashMap;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.cursors.IntIntCursor;

/**

 The <b>CollapsingQParserPlugin</b> is a PostFilter that performs field collapsing.
 This is a high performance alternative to standard Solr
 field collapsing (with ngroups) when the number of distinct groups
 in the result set is high.
 <p/>
 Sample syntax:
 <p/>
 Collapse based on the highest scoring document:
 <p/>

 fq=(!collapse field=field_name}

 <p/>
 Collapse based on the min value of a numeric field:
 <p/>
 fq={!collapse field=field_name min=field_name}
 <p/>
 Collapse based on the max value of a numeric field:
 <p/>
 fq={!collapse field=field_name max=field_name}
 <p/>
 Collapse with a null policy:
 <p/>
 fq={!collapse field=field_name nullPolicy=nullPolicy}
 <p/>
 There are three null policies: <br/>
 ignore : removes docs with a null value in the collapse field (default).<br/>
 expand : treats each doc with a null value in the collapse field as a separate group.<br/>
 collapse : collapses all docs with a null value into a single group using either highest score, or min/max.
 <p/>
 The CollapsingQParserPlugin fully supports the QueryElevationComponent


 **/

public class CollapsingQParserPlugin extends QParserPlugin {

  public static final String NAME = "collapse";
  public static final String NULL_COLLAPSE = "collapse";
  public static final String NULL_IGNORE = "ignore";
  public static final String NULL_EXPAND = "expand";


  public void init(NamedList namedList) {

  }

  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
    return new CollapsingQParser(qstr, localParams, params, request);
  }

  private class CollapsingQParser extends QParser {

    public CollapsingQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
      super(qstr, localParams, params, request);
    }

    public Query parse() throws SyntaxError {
      try {
        return new CollapsingPostFilter(localParams, params, req);
      } catch (Exception e) {
        throw new SyntaxError(e.getMessage(), e);
      }
    }
  }

  public class CollapsingPostFilter extends ExtendedQueryBase implements PostFilter, ScoreFilter {

    private String field;
    private String max;
    private String min;
    private boolean needsScores = true;
    private int nullPolicy;
    private Map<BytesRef, Integer> boosted;
    public static final int NULL_POLICY_IGNORE = 0;
    public static final int NULL_POLICY_COLLAPSE = 1;
    public static final int NULL_POLICY_EXPAND = 2;


    public String getField(){
      return this.field;
    }

    public void setCache(boolean cache) {

    }

    public void setCacheSep(boolean cacheSep) {

    }

    public boolean getCacheSep() {
      return false;
    }

    public boolean getCache() {
      return false;
    }

    public int hashCode() {
      int hashCode = field.hashCode();
      hashCode = max!=null ? hashCode+max.hashCode():hashCode;
      hashCode = min!=null ? hashCode+min.hashCode():hashCode;
      hashCode = hashCode+nullPolicy;
      hashCode = hashCode*((1+Float.floatToIntBits(this.getBoost()))*31);
      return hashCode;
    }

    public boolean equals(Object o) {

      if(o instanceof CollapsingPostFilter) {
        CollapsingPostFilter c = (CollapsingPostFilter)o;
        if(this.field.equals(c.field) &&
           ((this.max == null && c.max == null) || (this.max != null && c.max != null && this.max.equals(c.max))) &&
           ((this.min == null && c.min == null) || (this.min != null && c.min != null && this.min.equals(c.min))) &&
           this.nullPolicy == c.nullPolicy &&
           this.getBoost()==c.getBoost()) {
          return true;
        }
      }
      return false;
    }

    public int getCost() {
      return Math.max(super.getCost(), 100);
    }

    public String toString(String s) {
      return s;
    }

    public CollapsingPostFilter(SolrParams localParams, SolrParams params, SolrQueryRequest request) throws IOException {
      this.field = localParams.get("field");
      if (this.field == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Required 'field' param is missing.");
      }
      this.max = localParams.get("max");
      this.min = localParams.get("min");
      if(this.min != null || this.max != null) {
        this.needsScores = needsScores(params);
      }

      String nPolicy = localParams.get("nullPolicy", NULL_IGNORE);
      if(nPolicy.equals(NULL_IGNORE)) {
        this.nullPolicy = NULL_POLICY_IGNORE;
      } else if (nPolicy.equals(NULL_COLLAPSE)) {
        this.nullPolicy = NULL_POLICY_COLLAPSE;
      } else if(nPolicy.equals((NULL_EXPAND))) {
        this.nullPolicy = NULL_POLICY_EXPAND;
      } else {
        throw new IOException("Invalid nullPolicy:"+nPolicy);
      }

    }

    private IntIntOpenHashMap getBoostDocs(SolrIndexSearcher indexSearcher, Map<BytesRef, Integer> boosted, Map context) throws IOException {
      IntIntOpenHashMap boostDocs = QueryElevationComponent.getBoostDocs(indexSearcher, boosted, context);
      return boostDocs;
    }

    public DelegatingCollector getFilterCollector(IndexSearcher indexSearcher) {
      try {

        SolrIndexSearcher searcher = (SolrIndexSearcher)indexSearcher;

        SortedDocValues docValues = null;
        FunctionQuery funcQuery = null;
        docValues = DocValues.getSorted(searcher.getLeafReader(), this.field);

        FieldType fieldType = null;

        if(this.max != null) {
          if(this.max.indexOf("(") == -1) {
            fieldType = searcher.getSchema().getField(this.max).getType();
          } else {
            LocalSolrQueryRequest request = null;
            try {
              SolrParams params = new ModifiableSolrParams();
              request = new LocalSolrQueryRequest(searcher.getCore(), params);
              FunctionQParser functionQParser = new FunctionQParser(this.max, null, null,request);
              funcQuery = (FunctionQuery)functionQParser.parse();
            } catch (Exception e) {
              throw new IOException(e);
            } finally {
              request.close();
            }
          }
        }

        if(this.min != null) {
          if(this.min.indexOf("(") == -1) {
            fieldType = searcher.getSchema().getField(this.min).getType();
          } else {
            LocalSolrQueryRequest request = null;
            try {
              SolrParams params = new ModifiableSolrParams();
              request = new LocalSolrQueryRequest(searcher.getCore(), params);
              FunctionQParser functionQParser = new FunctionQParser(this.min, null, null,request);
              funcQuery = (FunctionQuery)functionQParser.parse();
            } catch (Exception e) {
              throw new IOException(e);
            } finally {
              request.close();
            }
          }
        }

        int maxDoc = searcher.maxDoc();
        int leafCount = searcher.getTopReaderContext().leaves().size();

        //Deal with boosted docs.
        //We have to deal with it here rather then the constructor because
        //because the QueryElevationComponent runs after the Queries are constructed.

        IntIntOpenHashMap boostDocs = null;
        Map context = null;
        SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
        if(info != null) {
          context = info.getReq().getContext();
        }

        if(this.boosted == null && context != null) {
          this.boosted = (Map<BytesRef, Integer>)context.get(QueryElevationComponent.BOOSTED_PRIORITY);
        }

        boostDocs = getBoostDocs(searcher, this.boosted, context);

        if (this.min != null || this.max != null) {

          return new CollapsingFieldValueCollector(maxDoc,
                                                   leafCount,
                                                   docValues,
                                                   this.nullPolicy,
                                                   max != null ? this.max : this.min,
                                                   max != null,
                                                   this.needsScores,
                                                   fieldType,
                                                   boostDocs,
                                                   funcQuery, searcher);
        } else {
          return new CollapsingScoreCollector(maxDoc, leafCount, docValues, this.nullPolicy, boostDocs);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private boolean needsScores(SolrParams params) {

      String sortSpec = params.get("sort");
      if(sortSpec != null && sortSpec.length()!=0) {
        String[] sorts = sortSpec.split(",");
        for(String s: sorts) {
          String parts[] = s.split(" ");
          if(parts[0].equals("score")) {
            return true;
          }
        }
      } else {
        //No sort specified so it defaults to score.
        return true;
      }

      String fl = params.get("fl");
      if(fl != null) {
        String[] fls = fl.split(",");
        for(String f : fls) {
          if(f.trim().equals("score")) {
            return true;
          }
        }
      }

      if(this.boosted != null) {
        return true;
      }

      return false;
    }
  }

  private class DummyScorer extends Scorer {

    public float score;
    public int docId;

    public DummyScorer() {
      super(null);
    }

    public float score() {
      return score;
    }

    public int freq() {
      return 0;
    }

    public int advance(int i) {
      return -1;
    }

    public int nextDoc() {
      return 0;
    }

    public int docID() {
      return docId;
    }

    public long cost() {
      return 0;
    }
  }


  private class CollapsingScoreCollector extends DelegatingCollector {

    private LeafReaderContext[] contexts;
    private FixedBitSet collapsedSet;
    private SortedDocValues values;
    private int[] ords;
    private float[] scores;
    private int maxDoc;
    private int nullPolicy;
    private float nullScore = -Float.MAX_VALUE;
    private int nullDoc;
    private FloatArrayList nullScores;
    private IntIntOpenHashMap boostDocs;
    private int[] boostOrds;

    public CollapsingScoreCollector(int maxDoc,
                                    int segments,
                                    SortedDocValues values,
                                    int nullPolicy,
                                    IntIntOpenHashMap boostDocs) {
      this.maxDoc = maxDoc;
      this.contexts = new LeafReaderContext[segments];
      this.collapsedSet = new FixedBitSet(maxDoc);
      this.boostDocs = boostDocs;
      if(this.boostDocs != null) {
        //Set the elevated docs now.
        IntOpenHashSet boostG = new IntOpenHashSet();
        Iterator<IntIntCursor> it = this.boostDocs.iterator();
        while(it.hasNext()) {
          IntIntCursor cursor = it.next();
          int i = cursor.key;
          this.collapsedSet.set(i);
          int ord = values.getOrd(i);
          if(ord > -1) {
            boostG.add(ord);
          }
        }
        boostOrds = boostG.toArray();
        Arrays.sort(boostOrds);
      }
      this.values = values;
      int valueCount = values.getValueCount();
      this.ords = new int[valueCount];
      Arrays.fill(this.ords, -1);
      this.scores = new float[valueCount];
      Arrays.fill(this.scores, -Float.MAX_VALUE);
      this.nullPolicy = nullPolicy;
      if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        nullScores = new FloatArrayList();
      }
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      //Documents must be sent in order to this collector.
      return false;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      this.contexts[context.ord] = context;
      this.docBase = context.docBase;
    }

    @Override
    public void collect(int docId) throws IOException {
      int globalDoc = docId+this.docBase;
      int ord = values.getOrd(globalDoc);

      if(ord > -1) {
        float score = scorer.score();
        if(score > scores[ord]) {
          ords[ord] = globalDoc;
          scores[ord] = score;
        }
      } else if (this.collapsedSet.get(globalDoc)) {
        //The doc is elevated so score does not matter
        //We just want to be sure it doesn't fall into the null policy
      } else if(nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        float score = scorer.score();
        if(score > nullScore) {
          nullScore = score;
          nullDoc = globalDoc;
        }
      } else if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        collapsedSet.set(globalDoc);
        nullScores.add(scorer.score());
      }
    }

    @Override
    public void finish() throws IOException {
      if(contexts.length == 0) {
        return;
      }

      if(nullScore > 0) {
        this.collapsedSet.set(nullDoc);
      }

      if(this.boostOrds != null) {
        for(int i=0; i<this.boostOrds.length; i++) {
          ords[boostOrds[i]] = -1;
        }
      }

      for(int i=0; i<ords.length; i++) {
        int doc = ords[i];
        if(doc > -1) {
          collapsedSet.set(doc);
        }
      }

      int currentContext = 0;
      int currentDocBase = 0;
      int nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
      leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
      DummyScorer dummy = new DummyScorer();
      leafDelegate.setScorer(dummy);
      DocIdSetIterator it = new FixedBitSetIterator(collapsedSet, 0L); // cost is not useful here
      int docId = -1;
      int nullScoreIndex = 0;
      while((docId = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {

        int ord = values.getOrd(docId);

        if(ord > -1) {
          dummy.score = scores[ord];
        } else if(this.boostDocs != null && boostDocs.containsKey(docId)) {
          //Elevated docs don't need a score.
          dummy.score = 0F;
        } else if (nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
          dummy.score = nullScore;
        } else if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          dummy.score = nullScores.get(nullScoreIndex++);
        }

        while(docId >= nextDocBase) {
          currentContext++;
          currentDocBase = contexts[currentContext].docBase;
          nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
          leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
          leafDelegate.setScorer(dummy);
        }

        int contextDoc = docId-currentDocBase;
        dummy.docId = contextDoc;
        leafDelegate.collect(contextDoc);
      }

      if(delegate instanceof DelegatingCollector) {
        ((DelegatingCollector) delegate).finish();
      }
    }
  }

  private class CollapsingFieldValueCollector extends DelegatingCollector {
    private LeafReaderContext[] contexts;
    private SortedDocValues values;

    private int maxDoc;
    private int nullPolicy;

    private FieldValueCollapse fieldValueCollapse;
    private boolean needsScores;
    private IntIntOpenHashMap boostDocs;

    public CollapsingFieldValueCollector(int maxDoc,
                                         int segments,
                                         SortedDocValues values,
                                         int nullPolicy,
                                         String field,
                                         boolean max,
                                         boolean needsScores,
                                         FieldType fieldType,
                                         IntIntOpenHashMap boostDocs,
                                         FunctionQuery funcQuery, IndexSearcher searcher) throws IOException{

      this.maxDoc = maxDoc;
      this.contexts = new LeafReaderContext[segments];
      this.values = values;
      int valueCount = values.getValueCount();
      this.nullPolicy = nullPolicy;
      this.needsScores = needsScores;
      this.boostDocs = boostDocs;
      if(funcQuery != null) {
        this.fieldValueCollapse =  new ValueSourceCollapse(maxDoc, field, nullPolicy, new int[valueCount], max, this.needsScores, boostDocs, funcQuery, searcher, values);
      } else {
        if(fieldType instanceof TrieIntField) {
          this.fieldValueCollapse = new IntValueCollapse(maxDoc, field, nullPolicy, new int[valueCount], max, this.needsScores, boostDocs, values);
        } else if(fieldType instanceof TrieLongField) {
          this.fieldValueCollapse =  new LongValueCollapse(maxDoc, field, nullPolicy, new int[valueCount], max, this.needsScores, boostDocs, values);
        } else if(fieldType instanceof TrieFloatField) {
          this.fieldValueCollapse =  new FloatValueCollapse(maxDoc, field, nullPolicy, new int[valueCount], max, this.needsScores, boostDocs, values);
        } else {
          throw new IOException("min/max must be either TrieInt, TrieLong or TrieFloat.");
        }
      }
    }

    public boolean acceptsDocsOutOfOrder() {
      //Documents must be sent in order to this collector.
      return false;
    }

    public void setScorer(Scorer scorer) {
      this.fieldValueCollapse.setScorer(scorer);
    }

    public void doSetNextReader(LeafReaderContext context) throws IOException {
      this.contexts[context.ord] = context;
      this.docBase = context.docBase;
      this.fieldValueCollapse.setNextReader(context);
    }

    public void collect(int docId) throws IOException {
      int globalDoc = docId+this.docBase;
      int ord = values.getOrd(globalDoc);
      fieldValueCollapse.collapse(ord, docId, globalDoc);
    }

    public void finish() throws IOException {
      if(contexts.length == 0) {
        return;
      }

      int currentContext = 0;
      int currentDocBase = 0;
      int nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
      leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
      DummyScorer dummy = new DummyScorer();
      leafDelegate.setScorer(dummy);
      DocIdSetIterator it = new FixedBitSetIterator(fieldValueCollapse.getCollapsedSet(), 0); // cost is not useful here
      int docId = -1;
      int nullScoreIndex = 0;
      float[] scores = fieldValueCollapse.getScores();
      FloatArrayList nullScores = fieldValueCollapse.getNullScores();
      float nullScore = fieldValueCollapse.getNullScore();
      while((docId = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {

        if(this.needsScores){
          int ord = values.getOrd(docId);
          if(ord > -1) {
            dummy.score = scores[ord];
          } else if (boostDocs != null && boostDocs.containsKey(docId)) {
            //Its an elevated doc so no score is needed
            dummy.score = 0F;
          } else if (nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
            dummy.score = nullScore;
          } else if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
            dummy.score = nullScores.get(nullScoreIndex++);
          }
        }

        while(docId >= nextDocBase) {
          currentContext++;
          currentDocBase = contexts[currentContext].docBase;
          nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
          leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
          leafDelegate.setScorer(dummy);
        }

        int contextDoc = docId-currentDocBase;
        dummy.docId = contextDoc;
        leafDelegate.collect(contextDoc);
      }

      if(delegate instanceof DelegatingCollector) {
        ((DelegatingCollector) delegate).finish();
      }
    }
  }

  private abstract class FieldValueCollapse {
    protected int nullPolicy;
    protected int[] ords;
    protected Scorer scorer;
    protected FloatArrayList nullScores;
    protected float nullScore;
    protected float[] scores;
    protected FixedBitSet collapsedSet;
    protected IntIntOpenHashMap boostDocs;
    protected int[] boostOrds;
    protected int nullDoc = -1;
    protected boolean needsScores;
    protected boolean max;
    protected String field;

    public abstract void collapse(int ord, int contextDoc, int globalDoc) throws IOException;
    public abstract void setNextReader(LeafReaderContext context) throws IOException;

    public FieldValueCollapse(int maxDoc,
                              String field,
                              int nullPolicy,
                              boolean max,
                              boolean needsScores,
                              IntIntOpenHashMap boostDocs,
                              SortedDocValues values) {
      this.field = field;
      this.nullPolicy = nullPolicy;
      this.max = max;
      this.needsScores = needsScores;
      this.collapsedSet = new FixedBitSet(maxDoc);
      this.boostDocs = boostDocs;
      if(this.boostDocs != null) {
        IntOpenHashSet boostG = new IntOpenHashSet();
        Iterator<IntIntCursor> it = boostDocs.iterator();
        while(it.hasNext()) {
          IntIntCursor cursor = it.next();
          int i = cursor.key;
          this.collapsedSet.set(i);
          int ord = values.getOrd(i);
          if(ord > -1) {
            boostG.add(ord);
          }
        }
        this.boostOrds = boostG.toArray();
        Arrays.sort(this.boostOrds);
      }
    }

    public FixedBitSet getCollapsedSet() {
      if(nullDoc > -1) {
        this.collapsedSet.set(nullDoc);
      }

      if(this.boostOrds != null) {
        for(int i=0; i<this.boostOrds.length; i++) {
          ords[boostOrds[i]] = -1;
        }
      }

      for(int i=0; i<ords.length; i++) {
        int doc = ords[i];
        if(doc > -1) {
          collapsedSet.set(doc);
        }
      }

      return collapsedSet;
    }

    public void setScorer(Scorer scorer) {
      this.scorer = scorer;
    }

    public FloatArrayList getNullScores() {
      return nullScores;
    }

    public float getNullScore() {
      return this.nullScore;
    }

    public float[] getScores() {
      return scores;
    }
  }

  private class IntValueCollapse extends FieldValueCollapse {

    private NumericDocValues vals;
    private IntCompare comp;
    private int nullVal;
    private int[] ordVals;

    public IntValueCollapse(int maxDoc,
                            String field,
                            int nullPolicy,
                            int[] ords,
                            boolean max,
                            boolean needsScores,
                            IntIntOpenHashMap boostDocs, SortedDocValues values) throws IOException {
      super(maxDoc, field, nullPolicy, max, needsScores, boostDocs, values);
      this.ords = ords;
      this.ordVals = new int[ords.length];
      Arrays.fill(ords, -1);

      if(max) {
        comp = new MaxIntComp();
        Arrays.fill(ordVals, Integer.MIN_VALUE);
      } else {
        comp = new MinIntComp();
        Arrays.fill(ordVals, Integer.MAX_VALUE);
        this.nullVal = Integer.MAX_VALUE;
      }

      if(needsScores) {
        this.scores = new float[ords.length];
        if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          nullScores = new FloatArrayList();
        }
      }
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.vals = DocValues.getNumeric(context.reader(), this.field);
    }

    public void collapse(int ord, int contextDoc, int globalDoc) throws IOException {
      int val = (int) vals.get(contextDoc);
      if(ord > -1) {
        if(comp.test(val, ordVals[ord])) {
          ords[ord] = globalDoc;
          ordVals[ord] = val;
          if(needsScores) {
            scores[ord] = scorer.score();
          }
        }
      } else if(this.collapsedSet.get(globalDoc)) {
        // Elevated doc so do nothing.
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if(comp.test(val, nullVal)) {
          nullVal = val;
          nullDoc = globalDoc;
          if(needsScores) {
            nullScore = scorer.score();
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        this.collapsedSet.set(globalDoc);
        if(needsScores) {
          nullScores.add(scorer.score());
        }
      }
    }
  }

  private class LongValueCollapse extends FieldValueCollapse {

    private NumericDocValues vals;
    private LongCompare comp;
    private long nullVal;
    private long[] ordVals;

    public LongValueCollapse(int maxDoc, String field,
                             int nullPolicy,
                             int[] ords,
                             boolean max,
                             boolean needsScores,
                             IntIntOpenHashMap boostDocs, SortedDocValues values) throws IOException {
      super(maxDoc, field, nullPolicy, max, needsScores, boostDocs, values);
      this.ords = ords;
      this.ordVals = new long[ords.length];
      Arrays.fill(ords, -1);

      if(max) {
        comp = new MaxLongComp();
        Arrays.fill(ordVals, Long.MIN_VALUE);
      } else {
        this.nullVal = Long.MAX_VALUE;
        comp = new MinLongComp();
        Arrays.fill(ordVals, Long.MAX_VALUE);
      }

      if(needsScores) {
        this.scores = new float[ords.length];
        if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          nullScores = new FloatArrayList();
        }
      }
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.vals = DocValues.getNumeric(context.reader(), this.field);
    }

    public void collapse(int ord, int contextDoc, int globalDoc) throws IOException {
      long val = vals.get(contextDoc);
      if(ord > -1) {
        if(comp.test(val, ordVals[ord])) {
          ords[ord] = globalDoc;
          ordVals[ord] = val;
          if(needsScores) {
            scores[ord] = scorer.score();
          }
        }
      } else if (this.collapsedSet.get(globalDoc)) {
        //Elevated doc so do nothing
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if(comp.test(val, nullVal)) {
          nullVal = val;
          nullDoc = globalDoc;
          if(needsScores) {
            nullScore = scorer.score();
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        this.collapsedSet.set(globalDoc);
        if(needsScores) {
          nullScores.add(scorer.score());
        }
      }
    }
  }

  private class FloatValueCollapse extends FieldValueCollapse {

    private NumericDocValues vals;
    private FloatCompare comp;
    private float nullVal;
    private float[] ordVals;

    public FloatValueCollapse(int maxDoc,
                              String field,
                              int nullPolicy,
                              int[] ords,
                              boolean max,
                              boolean needsScores,
                              IntIntOpenHashMap boostDocs, SortedDocValues values) throws IOException {
      super(maxDoc, field, nullPolicy, max, needsScores, boostDocs, values);
      this.ords = ords;
      this.ordVals = new float[ords.length];
      Arrays.fill(ords, -1);

      if(max) {
        comp = new MaxFloatComp();
        Arrays.fill(ordVals, -Float.MAX_VALUE );
      } else {
        this.nullVal = Float.MAX_VALUE;
        comp = new MinFloatComp();
        Arrays.fill(ordVals, Float.MAX_VALUE);
      }

      if(needsScores) {
        this.scores = new float[ords.length];
        if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          nullScores = new FloatArrayList();
        }
      }
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.vals = DocValues.getNumeric(context.reader(), this.field);
    }

    public void collapse(int ord, int contextDoc, int globalDoc) throws IOException {
      float val = Float.intBitsToFloat((int)vals.get(contextDoc));
      if(ord > -1) {
        if(comp.test(val, ordVals[ord])) {
          ords[ord] = globalDoc;
          ordVals[ord] = val;
          if(needsScores) {
            scores[ord] = scorer.score();
          }
        }
      } else if (this.collapsedSet.get(globalDoc)) {
        //Elevated doc so do nothing
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if(comp.test(val, nullVal)) {
          nullVal = val;
          nullDoc = globalDoc;
          if(needsScores) {
            nullScore = scorer.score();
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        this.collapsedSet.set(globalDoc);
        if(needsScores) {
          nullScores.add(scorer.score());
        }
      }
    }
  }

  private class ValueSourceCollapse extends FieldValueCollapse {

    private FloatCompare comp;
    private float nullVal;
    private ValueSource valueSource;
    private FunctionValues functionValues;
    private float[] ordVals;
    private Map rcontext;
    private CollapseScore collapseScore = new CollapseScore();
    private float score;
    private boolean cscore;

    public ValueSourceCollapse(int maxDoc,
                               String funcStr,
                               int nullPolicy,
                               int[] ords,
                               boolean max,
                               boolean needsScores,
                               IntIntOpenHashMap boostDocs,
                               FunctionQuery funcQuery, IndexSearcher searcher, SortedDocValues values) throws IOException {
      super(maxDoc, null, nullPolicy, max, needsScores, boostDocs, values);
      this.valueSource = funcQuery.getValueSource();
      this.rcontext = ValueSource.newContext(searcher);
      this.ords = ords;
      this.ordVals = new float[ords.length];
      Arrays.fill(ords, -1);

      if(max) {
        comp = new MaxFloatComp();
        Arrays.fill(ordVals, -Float.MAX_VALUE );
      } else {
        this.nullVal = Float.MAX_VALUE;
        comp = new MinFloatComp();
        Arrays.fill(ordVals, Float.MAX_VALUE);
      }

      if(funcStr.indexOf("cscore()") != -1) {
        this.cscore = true;
        this.rcontext.put("CSCORE",this.collapseScore);
      }

      if(this.needsScores) {
        this.scores = new float[ords.length];
        if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          nullScores = new FloatArrayList();
        }
      }
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      functionValues = this.valueSource.getValues(rcontext, context);
    }

    public void collapse(int ord, int contextDoc, int globalDoc) throws IOException {
      if(needsScores || cscore) {
        this.score = scorer.score();
        this.collapseScore.score = score;
      }

      float val = functionValues.floatVal(contextDoc);

      if(ord > -1) {
        if(comp.test(val, ordVals[ord])) {
          ords[ord] = globalDoc;
          ordVals[ord] = val;
          if(needsScores) {
            scores[ord] = score;
          }
        }
      } else if (this.collapsedSet.get(globalDoc)) {
        //Elevated doc so do nothing
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if(comp.test(val, nullVal)) {
          nullVal = val;
          nullDoc = globalDoc;
          if(needsScores) {
            nullScore = score;
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        this.collapsedSet.set(globalDoc);
        if(needsScores) {
          nullScores.add(score);
        }
      }
    }
  }

  public static final class CollapseScore {
    public float score;
  }

  private interface IntCompare {
    public boolean test(int i1, int i2);
  }

  private interface FloatCompare {
    public boolean test(float i1, float i2);
  }

  private interface LongCompare {
    public boolean test(long i1, long i2);
  }

  private class MaxIntComp implements IntCompare {
    public boolean test(int i1, int i2) {
      return i1 > i2;
    }
  }

  private class MinIntComp implements IntCompare {
    public boolean test(int i1, int i2) {
      return i1 < i2;
    }
  }

  private class MaxFloatComp implements FloatCompare {
    public boolean test(float i1, float i2) {
      return i1 > i2;
    }
  }

  private class MinFloatComp implements FloatCompare {
    public boolean test(float i1, float i2) {
      return i1 < i2;
    }
  }

  private class MaxLongComp implements LongCompare {
    public boolean test(long i1, long i2) {
      return i1 > i2;
    }
  }

  private class MinLongComp implements LongCompare {
    public boolean test(long i1, long i2) {
      return i1 < i2;
    }
  }
}

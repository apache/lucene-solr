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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.carrotsearch.hppc.FloatArrayList;
import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import com.carrotsearch.hppc.cursors.IntLongCursor;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.QueryElevationComponent;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TrieFloatField;
import org.apache.solr.schema.TrieIntField;
import org.apache.solr.schema.TrieLongField;

/**

 The <b>CollapsingQParserPlugin</b> is a PostFilter that performs field collapsing.
 This is a high performance alternative to standard Solr
 field collapsing (with ngroups) when the number of distinct groups
 in the result set is high.
 <p>
 Sample syntax:
 <p>
 Collapse based on the highest scoring document:
 <p>

 fq=(!collapse field=field_name}

 <p>
 Collapse based on the min value of a numeric field:
 <p>
 fq={!collapse field=field_name min=field_name}
 <p>
 Collapse based on the max value of a numeric field:
 <p>
 fq={!collapse field=field_name max=field_name}
 <p>
 Collapse with a null policy:
 <p>
 fq={!collapse field=field_name nullPolicy=nullPolicy}
 <p>
 There are three null policies: <br>
 ignore : removes docs with a null value in the collapse field (default).<br>
 expand : treats each doc with a null value in the collapse field as a separate group.<br>
 collapse : collapses all docs with a null value into a single group using either highest score, or min/max.
 <p>
 The CollapsingQParserPlugin fully supports the QueryElevationComponent
 **/

public class CollapsingQParserPlugin extends QParserPlugin {

  public static final String NAME = "collapse";
  public static final String NULL_COLLAPSE = "collapse";
  public static final String NULL_IGNORE = "ignore";
  public static final String NULL_EXPAND = "expand";
  public static final String HINT_TOP_FC = "top_fc";
  public static final String HINT_MULTI_DOCVALUES = "multi_docvalues";


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

    private String collapseField;
    private String max;
    private String min;
    public String hint;
    private boolean needsScores = true;
    private int nullPolicy;
    private Map<BytesRef, Integer> boosted;
    public static final int NULL_POLICY_IGNORE = 0;
    public static final int NULL_POLICY_COLLAPSE = 1;
    public static final int NULL_POLICY_EXPAND = 2;
    private int size;

    public String getField(){
      return this.collapseField;
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
      int hashCode = super.hashCode();
      hashCode = 31 * hashCode + collapseField.hashCode();
      hashCode = max!=null ? hashCode+max.hashCode():hashCode;
      hashCode = min!=null ? hashCode+min.hashCode():hashCode;
      hashCode = hashCode+nullPolicy;
      return hashCode;
    }

    public boolean equals(Object o) {
      if (super.equals(o) == false) {
        return false;
      }

      CollapsingPostFilter c = (CollapsingPostFilter)o;
      if(this.collapseField.equals(c.collapseField) &&
         ((this.max == null && c.max == null) || (this.max != null && c.max != null && this.max.equals(c.max))) &&
         ((this.min == null && c.min == null) || (this.min != null && c.min != null && this.min.equals(c.min))) &&
         this.nullPolicy == c.nullPolicy) {
        return true;
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
      this.collapseField = localParams.get("field");
      if (this.collapseField == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Required 'field' param is missing.");
      }
      this.max = localParams.get("max");
      this.min = localParams.get("min");
      this.hint = localParams.get("hint");
      this.size = localParams.getInt("size", 100000); //Only used for collapsing on int fields.

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

    private IntIntHashMap getBoostDocs(SolrIndexSearcher indexSearcher, Map<BytesRef, Integer> boosted, Map context) throws IOException {
      IntIntHashMap boostDocs = QueryElevationComponent.getBoostDocs(indexSearcher, boosted, context);
      return boostDocs;
    }

    public DelegatingCollector getFilterCollector(IndexSearcher indexSearcher) {
      try {

        SolrIndexSearcher searcher = (SolrIndexSearcher)indexSearcher;
        CollectorFactory collectorFactory = new CollectorFactory();
        //Deal with boosted docs.
        //We have to deal with it here rather then the constructor because
        //because the QueryElevationComponent runs after the Queries are constructed.

        IntIntHashMap boostDocsMap = null;
        Map context = null;
        SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
        if(info != null) {
          context = info.getReq().getContext();
        }

        if(this.boosted == null && context != null) {
          this.boosted = (Map<BytesRef, Integer>)context.get(QueryElevationComponent.BOOSTED_PRIORITY);
        }

        boostDocsMap = getBoostDocs(searcher, this.boosted, context);
        return collectorFactory.getCollector(this.collapseField,
                                             this.min,
                                             this.max,
                                             this.nullPolicy,
                                             this.hint,
                                             this.needsScores,
                                             this.size,
                                             boostDocsMap,
                                             searcher);

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

  private class ReaderWrapper extends FilterLeafReader {

    private String field;

    public ReaderWrapper(LeafReader leafReader, String field) {
      super(leafReader);
      this.field = field;
    }

    public SortedDocValues getSortedDocValues(String field) {
      return null;
    }

    public Object getCoreCacheKey() {
      return in.getCoreCacheKey();
    }

    public FieldInfos getFieldInfos() {
      Iterator<FieldInfo> it = in.getFieldInfos().iterator();
      List<FieldInfo> newInfos = new ArrayList();
      while(it.hasNext()) {
        FieldInfo fieldInfo = it.next();

        if(fieldInfo.name.equals(field)) {
          FieldInfo f = new FieldInfo(fieldInfo.name,
                                      fieldInfo.number,
                                      fieldInfo.hasVectors(),
                                      fieldInfo.hasNorms(),
                                      fieldInfo.hasPayloads(),
                                      fieldInfo.getIndexOptions(),
                                      DocValuesType.NONE,
                                      fieldInfo.getDocValuesGen(),
                                      fieldInfo.attributes());
          newInfos.add(f);

        } else {
          newInfos.add(fieldInfo);
        }
      }
      FieldInfos infos = new FieldInfos(newInfos.toArray(new FieldInfo[newInfos.size()]));
      return infos;
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



  /*
  * Collapses on Ordinal Values using Score to select the group head.
  */

  private class OrdScoreCollector extends DelegatingCollector {

    private LeafReaderContext[] contexts;
    private FixedBitSet collapsedSet;
    private SortedDocValues collapseValues;
    private MultiDocValues.OrdinalMap ordinalMap;
    private SortedDocValues segmentValues;
    private LongValues segmentOrdinalMap;
    private MultiDocValues.MultiSortedDocValues multiSortedDocValues;
    private int[] ords;
    private float[] scores;
    private int maxDoc;
    private int nullPolicy;
    private float nullScore = -Float.MAX_VALUE;
    private int nullDoc;
    private FloatArrayList nullScores;
    private IntArrayList boostOrds;
    private IntArrayList boostDocs;
    private MergeBoost mergeBoost;
    private boolean boosts;

    public OrdScoreCollector(int maxDoc,
                             int segments,
                             SortedDocValues collapseValues,
                             int nullPolicy,
                             IntIntHashMap boostDocsMap) {
      this.maxDoc = maxDoc;
      this.contexts = new LeafReaderContext[segments];
      this.collapsedSet = new FixedBitSet(maxDoc);
      this.collapseValues = collapseValues;
      int valueCount = collapseValues.getValueCount();
      if(collapseValues instanceof MultiDocValues.MultiSortedDocValues) {
        this.multiSortedDocValues = (MultiDocValues.MultiSortedDocValues)collapseValues;
        this.ordinalMap = multiSortedDocValues.mapping;
      }
      this.ords = new int[valueCount];
      Arrays.fill(this.ords, -1);
      this.scores = new float[valueCount];
      Arrays.fill(this.scores, -Float.MAX_VALUE);
      this.nullPolicy = nullPolicy;
      if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        nullScores = new FloatArrayList();
      }

      if(boostDocsMap != null) {
        this.boosts = true;
        this.boostOrds = new IntArrayList();
        this.boostDocs = new IntArrayList();
        int[] bd = new int[boostDocsMap.size()];
        Iterator<IntIntCursor> it =  boostDocsMap.iterator();
        int index = -1;
        while(it.hasNext()) {
          IntIntCursor cursor = it.next();
          bd[++index] = cursor.key;
        }

        Arrays.sort(bd);
        this.mergeBoost = new MergeBoost(bd);
      }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      this.contexts[context.ord] = context;
      this.docBase = context.docBase;
      if(ordinalMap != null) {
        this.segmentValues = this.multiSortedDocValues.values[context.ord];
        this.segmentOrdinalMap = ordinalMap.getGlobalOrds(context.ord);
      } else {
        this.segmentValues = collapseValues;
      }
    }

    @Override
    public void collect(int contextDoc) throws IOException {
      int globalDoc = contextDoc+this.docBase;
      int ord = -1;
      if(this.ordinalMap != null) {
        //Handle ordinalMapping case
        ord = segmentValues.getOrd(contextDoc);
        if(ord > -1) {
          ord = (int)segmentOrdinalMap.get(ord);
        }
      } else {
        //Handle top Level FieldCache or Single Segment Case
        ord = segmentValues.getOrd(globalDoc);
      }

      // Check to see if we have documents boosted by the QueryElevationComponent
      if(boosts && mergeBoost.boost(globalDoc)) {
        boostDocs.add(globalDoc);
        boostOrds.add(ord);
        return;
      }

      if(ord > -1) {
        float score = scorer.score();
        if(score > scores[ord]) {
          ords[ord] = globalDoc;
          scores[ord] = score;
        }
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
        collapsedSet.set(nullDoc);
      }

      //Handle the boosted docs.
      if(this.boostOrds != null) {
        int s = boostOrds.size();
        for(int i=0; i<s; i++) {
          int ord = this.boostOrds.get(i);
          if(ord > -1) {
            //Remove any group heads that are in the same groups as boosted documents.
            ords[ord] = -1;
          }
          //Add the boosted docs to the collapsedSet
          this.collapsedSet.set(boostDocs.get(i));
        }
        mergeBoost.reset(); // Reset mergeBoost because we're going to use it again.
      }

      //Build the sorted DocSet of group heads.
      for(int i=0; i<ords.length; i++) {
        int doc = ords[i];
        if(doc > -1) {
          collapsedSet.set(doc);
        }
      }

      int currentContext = 0;
      int currentDocBase = 0;

      if(ordinalMap != null) {
        this.segmentValues = this.multiSortedDocValues.values[currentContext];
        this.segmentOrdinalMap = this.ordinalMap.getGlobalOrds(currentContext);
      } else {
        this.segmentValues = collapseValues;
      }

      int nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
      leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
      DummyScorer dummy = new DummyScorer();
      leafDelegate.setScorer(dummy);
      DocIdSetIterator it = new BitSetIterator(collapsedSet, 0L); // cost is not useful here
      int docId = -1;
      int index = -1;
      while((docId = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {

        while(docId >= nextDocBase) {
          currentContext++;
          currentDocBase = contexts[currentContext].docBase;
          nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
          leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
          leafDelegate.setScorer(dummy);
          if(ordinalMap != null) {
            this.segmentValues = this.multiSortedDocValues.values[currentContext];
            this.segmentOrdinalMap = this.ordinalMap.getGlobalOrds(currentContext);
          }
        }

        int contextDoc = docId-currentDocBase;

        int ord = -1;
        if(this.ordinalMap != null) {
          //Handle ordinalMapping case
          ord = segmentValues.getOrd(contextDoc);
          if(ord > -1) {
            ord = (int)segmentOrdinalMap.get(ord);
          }
        } else {
          //Handle top Level FieldCache or Single Segment Case
          ord = segmentValues.getOrd(docId);
        }

        if(ord > -1) {
          dummy.score = scores[ord];
        } else if(boosts && mergeBoost.boost(docId)) {
          //Ignore so it doesn't mess up the null scoring.
        } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
          dummy.score = nullScore;
        } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          dummy.score = nullScores.get(++index);
        }

        dummy.docId = contextDoc;
        leafDelegate.collect(contextDoc);
      }

      if(delegate instanceof DelegatingCollector) {
        ((DelegatingCollector) delegate).finish();
      }
    }
  }

  /*
  * Collapses on an integer field using the score to select the group head.
  */

  private class IntScoreCollector extends DelegatingCollector {

    private LeafReaderContext[] contexts;
    private FixedBitSet collapsedSet;
    private NumericDocValues collapseValues;
    private IntLongHashMap cmap;
    private int maxDoc;
    private int nullPolicy;
    private float nullScore = -Float.MAX_VALUE;
    private int nullDoc;
    private FloatArrayList nullScores;
    private IntArrayList boostKeys;
    private IntArrayList boostDocs;
    private MergeBoost mergeBoost;
    private boolean boosts;
    private String field;
    private int nullValue;

    public IntScoreCollector(int maxDoc,
                             int segments,
                             int nullValue,
                             int nullPolicy,
                             int size,
                             String field,
                             IntIntHashMap boostDocsMap) {
      this.maxDoc = maxDoc;
      this.contexts = new LeafReaderContext[segments];
      this.collapsedSet = new FixedBitSet(maxDoc);
      this.nullValue = nullValue;
      this.nullPolicy = nullPolicy;
      if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        nullScores = new FloatArrayList();
      }
      this.cmap = new IntLongHashMap(size);
      this.field = field;

      if(boostDocsMap != null) {
        this.boosts = true;
        this.boostDocs = new IntArrayList();
        this.boostKeys = new IntArrayList();
        int[] bd = new int[boostDocsMap.size()];
        Iterator<IntIntCursor> it =  boostDocsMap.iterator();
        int index = -1;
        while(it.hasNext()) {
          IntIntCursor cursor = it.next();
          bd[++index] = cursor.key;
        }

        Arrays.sort(bd);
        this.mergeBoost = new MergeBoost(bd);
        this.boosts = true;
      }

    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      this.contexts[context.ord] = context;
      this.docBase = context.docBase;
      this.collapseValues = DocValues.getNumeric(context.reader(), this.field);
    }

    @Override
    public void collect(int contextDoc) throws IOException {

      int collapseValue = (int)this.collapseValues.get(contextDoc);
      int globalDoc = docBase+contextDoc;

      // Check to see of we have documents boosted by the QueryElevationComponent
      if(boosts && mergeBoost.boost(globalDoc)) {
        boostDocs.add(globalDoc);
        boostKeys.add(collapseValue);
        return;
      }

      if(collapseValue != nullValue) {
        float score = scorer.score();
        final int idx;
        if((idx = cmap.indexOf(collapseValue)) >= 0) {
          long scoreDoc = cmap.indexGet(idx);
          int testScore = (int)(scoreDoc>>32);
          int currentScore = Float.floatToRawIntBits(score);
          if(currentScore > testScore) {
            //Current score is higher so replace the old scoreDoc with the current scoreDoc
            cmap.indexReplace(idx, (((long)currentScore)<<32)+globalDoc);
          }
        } else {
          //Combine the score and document into a long.
          long scoreDoc = (((long)Float.floatToRawIntBits(score))<<32)+globalDoc;
          cmap.indexInsert(idx, collapseValue, scoreDoc);
        }
      } else if(nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        float score = scorer.score();
        if(score > this.nullScore) {
          this.nullScore = score;
          this.nullDoc = globalDoc;
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

      if(nullScore > -1) {
        collapsedSet.set(nullDoc);
      }

      //Handle the boosted docs.
      if(this.boostKeys != null) {
        int s = boostKeys.size();
        for(int i=0; i<s; i++) {
          int key = this.boostKeys.get(i);
          if(key != nullValue) {
            cmap.remove(key);
          }
          //Add the boosted docs to the collapsedSet
          this.collapsedSet.set(boostDocs.get(i));
        }
      }

      Iterator<IntLongCursor> it1 = cmap.iterator();

      while(it1.hasNext()) {
        IntLongCursor cursor = it1.next();
        int doc = (int)cursor.value;
        collapsedSet.set(doc);
      }

      int currentContext = 0;
      int currentDocBase = 0;

      collapseValues = contexts[currentContext].reader().getNumericDocValues(this.field);
      int nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
      leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
      DummyScorer dummy = new DummyScorer();
      leafDelegate.setScorer(dummy);
      DocIdSetIterator it = new BitSetIterator(collapsedSet, 0L); // cost is not useful here
      int globalDoc = -1;
      int nullScoreIndex = 0;
      while((globalDoc = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {

        while(globalDoc >= nextDocBase) {
          currentContext++;
          currentDocBase = contexts[currentContext].docBase;
          nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
          leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
          leafDelegate.setScorer(dummy);
          collapseValues = contexts[currentContext].reader().getNumericDocValues(this.field);
        }

        int contextDoc = globalDoc-currentDocBase;

        int collapseValue = (int)collapseValues.get(contextDoc);
        if(collapseValue != nullValue) {
          long scoreDoc = cmap.get(collapseValue);
          dummy.score = Float.intBitsToFloat((int)(scoreDoc>>32));
        } else if(boosts && mergeBoost.boost(globalDoc)) {
          //Ignore so boosted documents don't mess up the null scoring policies.
        } else if (nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
          dummy.score = nullScore;
        } else if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          dummy.score = nullScores.get(nullScoreIndex++);
        }

        dummy.docId = contextDoc;
        leafDelegate.collect(contextDoc);
      }

      if(delegate instanceof DelegatingCollector) {
        ((DelegatingCollector) delegate).finish();
      }
    }
  }
  /*
  *  Collapse on Ordinal value using max/min value of a field to select the group head.
  */

  private class OrdFieldValueCollector extends DelegatingCollector {
    private LeafReaderContext[] contexts;
    private SortedDocValues collapseValues;
    protected MultiDocValues.OrdinalMap ordinalMap;
    protected SortedDocValues segmentValues;
    protected LongValues segmentOrdinalMap;
    protected MultiDocValues.MultiSortedDocValues multiSortedDocValues;

    private int maxDoc;
    private int nullPolicy;

    private OrdFieldValueStrategy collapseStrategy;
    private boolean needsScores;

    public OrdFieldValueCollector(int maxDoc,
                                  int segments,
                                  SortedDocValues collapseValues,
                                  int nullPolicy,
                                  String field,
                                  boolean max,
                                  boolean needsScores,
                                  FieldType fieldType,
                                  IntIntHashMap boostDocs,
                                  FunctionQuery funcQuery, IndexSearcher searcher) throws IOException{

      this.maxDoc = maxDoc;
      this.contexts = new LeafReaderContext[segments];
      this.collapseValues = collapseValues;
      if(collapseValues instanceof MultiDocValues.MultiSortedDocValues) {
        this.multiSortedDocValues = (MultiDocValues.MultiSortedDocValues)collapseValues;
        this.ordinalMap = multiSortedDocValues.mapping;
      }

      int valueCount = collapseValues.getValueCount();
      this.nullPolicy = nullPolicy;
      this.needsScores = needsScores;
      if(funcQuery != null) {
        this.collapseStrategy =  new OrdValueSourceStrategy(maxDoc, field, nullPolicy, new int[valueCount], max, this.needsScores, boostDocs, funcQuery, searcher, collapseValues);
      } else {
        if(fieldType instanceof TrieIntField) {
          this.collapseStrategy = new OrdIntStrategy(maxDoc, field, nullPolicy, new int[valueCount], max, this.needsScores, boostDocs, collapseValues);
        } else if(fieldType instanceof TrieFloatField) {
          this.collapseStrategy = new OrdFloatStrategy(maxDoc, field, nullPolicy, new int[valueCount], max, this.needsScores, boostDocs, collapseValues);
        } else if(fieldType instanceof TrieLongField) {
          this.collapseStrategy =  new OrdLongStrategy(maxDoc, field, nullPolicy, new int[valueCount], max, this.needsScores, boostDocs, collapseValues);
        } else {
          throw new IOException("min/max must be either TrieInt, TrieLong, TrieFloat.");
        }
      }
    }

    public boolean acceptsDocsOutOfOrder() {
      //Documents must be sent in order to this collector.
      return false;
    }

    public void setScorer(Scorer scorer) {
      this.collapseStrategy.setScorer(scorer);
    }

    public void doSetNextReader(LeafReaderContext context) throws IOException {
      this.contexts[context.ord] = context;
      this.docBase = context.docBase;
      this.collapseStrategy.setNextReader(context);
      if(ordinalMap != null) {
        this.segmentValues = this.multiSortedDocValues.values[context.ord];
        this.segmentOrdinalMap = ordinalMap.getGlobalOrds(context.ord);
      } else {
        this.segmentValues = collapseValues;
      }
    }

    public void collect(int contextDoc) throws IOException {
      int globalDoc = contextDoc+this.docBase;
      int ord = -1;
      if(this.ordinalMap != null) {
        ord = segmentValues.getOrd(contextDoc);
        if(ord > -1) {
          ord = (int)segmentOrdinalMap.get(ord);
        }
      } else {
        ord = segmentValues.getOrd(globalDoc);
      }
      collapseStrategy.collapse(ord, contextDoc, globalDoc);
    }

    public void finish() throws IOException {
      if(contexts.length == 0) {
        return;
      }

      int currentContext = 0;
      int currentDocBase = 0;

      if(ordinalMap != null) {
        this.segmentValues = this.multiSortedDocValues.values[currentContext];
        this.segmentOrdinalMap = this.ordinalMap.getGlobalOrds(currentContext);
      } else {
        this.segmentValues = collapseValues;
      }

      int nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
      leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
      DummyScorer dummy = new DummyScorer();
      leafDelegate.setScorer(dummy);
      DocIdSetIterator it = new BitSetIterator(collapseStrategy.getCollapsedSet(), 0); // cost is not useful here
      int globalDoc = -1;
      int nullScoreIndex = 0;
      float[] scores = collapseStrategy.getScores();
      FloatArrayList nullScores = collapseStrategy.getNullScores();
      float nullScore = collapseStrategy.getNullScore();

      MergeBoost mergeBoost = collapseStrategy.getMergeBoost();
      while((globalDoc = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {

        while(globalDoc >= nextDocBase) {
          currentContext++;
          currentDocBase = contexts[currentContext].docBase;
          nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
          leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
          leafDelegate.setScorer(dummy);
          if(ordinalMap != null) {
            this.segmentValues = this.multiSortedDocValues.values[currentContext];
            this.segmentOrdinalMap = this.ordinalMap.getGlobalOrds(currentContext);
          }
        }

        int contextDoc = globalDoc-currentDocBase;

        if(this.needsScores){
          int ord = -1;
          if(this.ordinalMap != null) {
            //Handle ordinalMapping case
            ord = segmentValues.getOrd(contextDoc);
            if(ord > -1) {
              ord = (int)segmentOrdinalMap.get(ord);
            }
          } else {
            //Handle top Level FieldCache or Single Segment Case
            ord = segmentValues.getOrd(globalDoc);
          }

          if(ord > -1) {
            dummy.score = scores[ord];
          } else if (mergeBoost != null && mergeBoost.boost(globalDoc)) {
            //It's an elevated doc so no score is needed
            dummy.score = 0F;
          } else if (nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
            dummy.score = nullScore;
          } else if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
            dummy.score = nullScores.get(nullScoreIndex++);
          }
        }

        dummy.docId = contextDoc;
        leafDelegate.collect(contextDoc);
      }

      if(delegate instanceof DelegatingCollector) {
        ((DelegatingCollector) delegate).finish();
      }
    }
  }


  /*
  *  Collapses on an integer field using the min/max value of numeric field to select the group head.
  */

  private class IntFieldValueCollector extends DelegatingCollector {
    private LeafReaderContext[] contexts;
    private NumericDocValues collapseValues;
    private int maxDoc;
    private int nullValue;
    private int nullPolicy;

    private IntFieldValueStrategy collapseStrategy;
    private boolean needsScores;
    private String collapseField;

    public IntFieldValueCollector(int maxDoc,
                                  int size,
                                  int segments,
                                  int nullValue,
                                  int nullPolicy,
                                  String collapseField,
                                  String field,
                                  boolean max,
                                  boolean needsScores,
                                  FieldType fieldType,
                                  IntIntHashMap boostDocsMap,
                                  FunctionQuery funcQuery,
                                  IndexSearcher searcher) throws IOException{

      this.maxDoc = maxDoc;
      this.contexts = new LeafReaderContext[segments];
      this.collapseField = collapseField;
      this.nullValue = nullValue;
      this.nullPolicy = nullPolicy;
      this.needsScores = needsScores;
      if(funcQuery != null) {
        this.collapseStrategy =  new IntValueSourceStrategy(maxDoc, field, size, collapseField, nullValue, nullPolicy, max, this.needsScores, boostDocsMap, funcQuery, searcher);
      } else {
        if(fieldType instanceof TrieIntField) {
          this.collapseStrategy = new IntIntStrategy(maxDoc, size, collapseField, field, nullValue, nullPolicy, max, this.needsScores, boostDocsMap);
        } else if(fieldType instanceof TrieFloatField) {
          this.collapseStrategy = new IntFloatStrategy(maxDoc, size, collapseField, field, nullValue, nullPolicy, max, this.needsScores, boostDocsMap);
        } else {
          throw new IOException("min/max must be TrieInt or TrieFloat when collapsing on numeric fields .");
        }
      }
    }

    public boolean acceptsDocsOutOfOrder() {
      //Documents must be sent in order to this collector.
      return false;
    }

    public void setScorer(Scorer scorer) {
      this.collapseStrategy.setScorer(scorer);
    }

    public void doSetNextReader(LeafReaderContext context) throws IOException {
      this.contexts[context.ord] = context;
      this.docBase = context.docBase;
      this.collapseStrategy.setNextReader(context);
      this.collapseValues = context.reader().getNumericDocValues(this.collapseField);
    }

    public void collect(int contextDoc) throws IOException {
      int globalDoc = contextDoc+this.docBase;
      int collapseKey = (int)this.collapseValues.get(contextDoc);
      collapseStrategy.collapse(collapseKey, contextDoc, globalDoc);
    }

    public void finish() throws IOException {
      if(contexts.length == 0) {
        return;
      }

      int currentContext = 0;
      int currentDocBase = 0;
      this.collapseValues = contexts[currentContext].reader().getNumericDocValues(this.collapseField);
      int nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
      leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
      DummyScorer dummy = new DummyScorer();
      leafDelegate.setScorer(dummy);
      DocIdSetIterator it = new BitSetIterator(collapseStrategy.getCollapsedSet(), 0); // cost is not useful here
      int globalDoc = -1;
      int nullScoreIndex = 0;
      IntIntHashMap cmap = collapseStrategy.getCollapseMap();
      int[] docs = collapseStrategy.getDocs();
      float[] scores = collapseStrategy.getScores();
      FloatArrayList nullScores = collapseStrategy.getNullScores();
      MergeBoost mergeBoost = collapseStrategy.getMergeBoost();
      float nullScore = collapseStrategy.getNullScore();

      while((globalDoc = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {

        while(globalDoc >= nextDocBase) {
          currentContext++;
          currentDocBase = contexts[currentContext].docBase;
          nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
          leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
          leafDelegate.setScorer(dummy);
          this.collapseValues = contexts[currentContext].reader().getNumericDocValues(this.collapseField);
        }

        int contextDoc = globalDoc-currentDocBase;

        if(this.needsScores){
          int collapseValue = (int)collapseValues.get(contextDoc);
          if(collapseValue != nullValue) {
            int pointer = cmap.get(collapseValue);
            dummy.score = scores[pointer];
          } else if (mergeBoost != null && mergeBoost.boost(globalDoc)) {
            //Its an elevated doc so no score is needed
            dummy.score = 0F;
          } else if (nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
            dummy.score = nullScore;
          } else if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
            dummy.score = nullScores.get(nullScoreIndex++);
          }
        }

        dummy.docId = contextDoc;
        leafDelegate.collect(contextDoc);
      }

      if(delegate instanceof DelegatingCollector) {
        ((DelegatingCollector) delegate).finish();
      }
    }
  }

  private class CollectorFactory {


    public DelegatingCollector getCollector(String collapseField,
                                            String min,
                                            String max,
                                            int nullPolicy,
                                            String hint,
                                            boolean needsScores,
                                            int size,
                                            IntIntHashMap boostDocs,
                                            SolrIndexSearcher searcher) throws IOException {



      SortedDocValues docValues = null;
      FunctionQuery funcQuery = null;

      FieldType collapseFieldType = searcher.getSchema().getField(collapseField).getType();
      String defaultValue = searcher.getSchema().getField(collapseField).getDefaultValue();

      if(collapseFieldType instanceof StrField) {
        if(HINT_TOP_FC.equals(hint)) {

            /*
            * This hint forces the use of the top level field cache for String fields.
            * This is VERY fast at query time but slower to warm and causes insanity.
            */

          Map<String, UninvertingReader.Type> mapping = new HashMap();
          mapping.put(collapseField, UninvertingReader.Type.SORTED);
          UninvertingReader uninvertingReader = new UninvertingReader(new ReaderWrapper(searcher.getLeafReader(), collapseField), mapping);
          docValues = uninvertingReader.getSortedDocValues(collapseField);
        } else {
          docValues = DocValues.getSorted(searcher.getLeafReader(), collapseField);
        }
      } else {
        if(HINT_TOP_FC.equals(hint)) {
          throw new IOException("top_fc hint is only supported when collapsing on String Fields");
        }
      }

      FieldType minMaxFieldType = null;
      if(max != null) {
        if(max.indexOf("(") == -1) {
          minMaxFieldType = searcher.getSchema().getField(max).getType();
        } else {
          LocalSolrQueryRequest request = null;
          try {
            SolrParams params = new ModifiableSolrParams();
            request = new LocalSolrQueryRequest(searcher.getCore(), params);
            FunctionQParser functionQParser = new FunctionQParser(max, null, null,request);
            funcQuery = (FunctionQuery)functionQParser.parse();
          } catch (Exception e) {
            throw new IOException(e);
          } finally {
            request.close();
          }
        }
      }

      if(min != null) {
        if(min.indexOf("(") == -1) {
          minMaxFieldType = searcher.getSchema().getField(min).getType();
        } else {
          LocalSolrQueryRequest request = null;
          try {
            SolrParams params = new ModifiableSolrParams();
            request = new LocalSolrQueryRequest(searcher.getCore(), params);
            FunctionQParser functionQParser = new FunctionQParser(min, null, null,request);
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

      if (min != null || max != null) {

        if(collapseFieldType instanceof StrField) {

          return new OrdFieldValueCollector(maxDoc,
                                            leafCount,
                                            docValues,
                                            nullPolicy,
                                            max != null ? max : min,
                                            max != null,
                                            needsScores,
                                            minMaxFieldType,
                                            boostDocs,
                                            funcQuery,
                                            searcher);

        } else if((collapseFieldType instanceof TrieIntField ||
                   collapseFieldType instanceof TrieFloatField)) {

          int nullValue = 0;

          if(collapseFieldType instanceof TrieFloatField) {
            if(defaultValue != null) {
              nullValue = Float.floatToIntBits(Float.parseFloat(defaultValue));
            } else {
              nullValue = Float.floatToIntBits(0.0f);
            }
          } else {
            if(defaultValue != null) {
              nullValue = Integer.parseInt(defaultValue);
            }
          }

          return new IntFieldValueCollector(maxDoc,
                                            size,
                                            leafCount,
                                            nullValue,
                                            nullPolicy,
                                            collapseField,
                                            max != null ? max : min,
                                            max != null,
                                            needsScores,
                                            minMaxFieldType,
                                            boostDocs,
                                            funcQuery,
                                            searcher);
        } else {
          throw new IOException("64 bit numeric collapse fields are not supported");
        }

      } else {

        if(collapseFieldType instanceof StrField) {

          return new OrdScoreCollector(maxDoc, leafCount, docValues, nullPolicy, boostDocs);

        } else if(collapseFieldType instanceof TrieIntField ||
                  collapseFieldType instanceof TrieFloatField) {

          int nullValue = 0;

          if(collapseFieldType instanceof TrieFloatField) {
            if(defaultValue != null) {
              nullValue = Float.floatToIntBits(Float.parseFloat(defaultValue));
            } else {
              nullValue = Float.floatToIntBits(0.0f);
            }
          } else {
            if(defaultValue != null) {
              nullValue = Integer.parseInt(defaultValue);
            }
          }

          return new IntScoreCollector(maxDoc, leafCount, nullValue, nullPolicy, size, collapseField, boostDocs);

        } else {
          throw new IOException("64 bit numeric collapse fields are not supported");
        }
      }
    }
  }

  public static final class CollapseScore {
    public float score;
  }


  /*
  * Collapse Strategies
  */

  /*
  * The abstract base Strategy for collapse strategies that collapse on an ordinal
  * using min/max field value to select the group head.
  *
  */

  private abstract class OrdFieldValueStrategy {
    protected int nullPolicy;
    protected int[] ords;
    protected Scorer scorer;
    protected FloatArrayList nullScores;
    protected float nullScore;
    protected float[] scores;
    protected FixedBitSet collapsedSet;
    protected int nullDoc = -1;
    protected boolean needsScores;
    protected boolean max;
    protected String field;
    protected boolean boosts;
    protected IntArrayList boostOrds;
    protected IntArrayList boostDocs;
    protected MergeBoost mergeBoost;
    protected boolean boosted;

    public abstract void collapse(int ord, int contextDoc, int globalDoc) throws IOException;
    public abstract void setNextReader(LeafReaderContext context) throws IOException;

    public OrdFieldValueStrategy(int maxDoc,
                                 String field,
                                 int nullPolicy,
                                 boolean max,
                                 boolean needsScores,
                                 IntIntHashMap boostDocsMap,
                                 SortedDocValues values) {
      this.field = field;
      this.nullPolicy = nullPolicy;
      this.max = max;
      this.needsScores = needsScores;
      this.collapsedSet = new FixedBitSet(maxDoc);
      if(boostDocsMap != null) {
        this.boosts = true;
        this.boostOrds = new IntArrayList();
        this.boostDocs = new IntArrayList();
        int[] bd = new int[boostDocsMap.size()];
        Iterator<IntIntCursor> it =  boostDocsMap.iterator();
        int index = -1;
        while(it.hasNext()) {
          IntIntCursor cursor = it.next();
          bd[++index] = cursor.key;
        }

        Arrays.sort(bd);
        this.mergeBoost = new MergeBoost(bd);
        this.boosted = true;
      }
    }

    public MergeBoost getMergeBoost() {
      return this.mergeBoost;
    }

    public FixedBitSet getCollapsedSet() {
      if(nullDoc > -1) {
        this.collapsedSet.set(nullDoc);
      }

      if(this.boostOrds != null) {
        int s = boostOrds.size();
        for(int i=0; i<s; i++) {
          int ord = boostOrds.get(i);
          if(ord > -1) {
            ords[ord] = -1;
          }
          collapsedSet.set(boostDocs.get(i));
        }

        mergeBoost.reset();
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

  /*
  * Strategy for collapsing on ordinal using min/max of an int field to select the group head.
  */

  private class OrdIntStrategy extends OrdFieldValueStrategy {

    private NumericDocValues minMaxValues;
    private IntCompare comp;
    private int nullVal;
    private int[] ordVals;

    public OrdIntStrategy(int maxDoc,
                          String field,
                          int nullPolicy,
                          int[] ords,
                          boolean max,
                          boolean needsScores,
                          IntIntHashMap boostDocs,
                          SortedDocValues values) throws IOException {
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
      this.minMaxValues = DocValues.getNumeric(context.reader(), this.field);
    }

    public void collapse(int ord, int contextDoc, int globalDoc) throws IOException {

      if(this.boosted && mergeBoost.boost(globalDoc)) {
        this.boostDocs.add(globalDoc);
        this.boostOrds.add(ord);
        return;
      }

      int currentVal = (int) minMaxValues.get(contextDoc);

      if(ord > -1) {
        if(comp.test(currentVal, ordVals[ord])) {
          ords[ord] = globalDoc;
          ordVals[ord] = currentVal;
          if(needsScores) {
            scores[ord] = scorer.score();
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if(comp.test(currentVal, nullVal)) {
          nullVal = currentVal;
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

  /*
  * Strategy for collapsing on ordinal and using the min/max value of a float
  * field to select the group head
  */

  private class OrdFloatStrategy extends OrdFieldValueStrategy {

    private NumericDocValues minMaxValues;
    private FloatCompare comp;
    private float nullVal;
    private float[] ordVals;

    public OrdFloatStrategy(int maxDoc,
                          String field,
                          int nullPolicy,
                          int[] ords,
                          boolean max,
                          boolean needsScores,
                          IntIntHashMap boostDocs,
                          SortedDocValues values) throws IOException {
      super(maxDoc, field, nullPolicy, max, needsScores, boostDocs, values);
      this.ords = ords;
      this.ordVals = new float[ords.length];
      Arrays.fill(ords, -1);

      if(max) {
        comp = new MaxFloatComp();
        Arrays.fill(ordVals, -Float.MAX_VALUE);
        this.nullVal = -Float.MAX_VALUE;
      } else {
        comp = new MinFloatComp();
        Arrays.fill(ordVals, Float.MAX_VALUE);
        this.nullVal = Float.MAX_VALUE;
      }

      if(needsScores) {
        this.scores = new float[ords.length];
        if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          nullScores = new FloatArrayList();
        }
      }
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.minMaxValues = DocValues.getNumeric(context.reader(), this.field);
    }

    public void collapse(int ord, int contextDoc, int globalDoc) throws IOException {

      if(this.boosted && mergeBoost.boost(globalDoc)) {
        this.boostDocs.add(globalDoc);
        this.boostOrds.add(ord);
        return;
      }

      int currentMinMax = (int) minMaxValues.get(contextDoc);
      float currentVal = Float.intBitsToFloat(currentMinMax);

      if(ord > -1) {
        if(comp.test(currentVal, ordVals[ord])) {
          ords[ord] = globalDoc;
          ordVals[ord] = currentVal;
          if(needsScores) {
            scores[ord] = scorer.score();
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if(comp.test(currentVal, nullVal)) {
          nullVal = currentVal;
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

  /*
  * Strategy for collapsing on ordinal and using the min/max value of a long
  * field to select the group head
  */

  private class OrdLongStrategy extends OrdFieldValueStrategy {

    private NumericDocValues minMaxVals;
    private LongCompare comp;
    private long nullVal;
    private long[] ordVals;

    public OrdLongStrategy(int maxDoc, String field,
                           int nullPolicy,
                           int[] ords,
                           boolean max,
                           boolean needsScores,
                           IntIntHashMap boostDocs, SortedDocValues values) throws IOException {
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
      this.minMaxVals = DocValues.getNumeric(context.reader(), this.field);
    }

    public void collapse(int ord, int contextDoc, int globalDoc) throws IOException {

      if(boosted && mergeBoost.boost(globalDoc)) {
        this.boostOrds.add(ord);
        this.boostDocs.add(globalDoc);
        return;
      }

      long currentVal = minMaxVals.get(contextDoc);
      if(ord > -1) {
        if(comp.test(currentVal, ordVals[ord])) {
          ords[ord] = globalDoc;
          ordVals[ord] = currentVal;
          if(needsScores) {
            scores[ord] = scorer.score();
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if(comp.test(currentVal, nullVal)) {
          nullVal = currentVal;
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

  /*
  * Strategy for collapsing on ordinal and using the min/max value of a value source function
  * to select the group head
  */

  private class OrdValueSourceStrategy extends OrdFieldValueStrategy {

    private FloatCompare comp;
    private float nullVal;
    private ValueSource valueSource;
    private FunctionValues functionValues;
    private float[] ordVals;
    private Map rcontext;
    private CollapseScore collapseScore = new CollapseScore();
    private float score;
    private boolean cscore;

    public OrdValueSourceStrategy(int maxDoc,
                                  String funcStr,
                                  int nullPolicy,
                                  int[] ords,
                                  boolean max,
                                  boolean needsScores,
                                  IntIntHashMap boostDocs,
                                  FunctionQuery funcQuery,
                                  IndexSearcher searcher,
                                  SortedDocValues values) throws IOException {
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

      if(boosted && mergeBoost.boost(globalDoc)) {
        this.boostOrds.add(ord);
        this.boostDocs.add(globalDoc);
      }

      if(needsScores || cscore) {
        this.score = scorer.score();
        this.collapseScore.score = score;
      }

      float currentVal = functionValues.floatVal(contextDoc);

      if(ord > -1) {
        if(comp.test(currentVal, ordVals[ord])) {
          ords[ord] = globalDoc;
          ordVals[ord] = currentVal;
          if(needsScores) {
            scores[ord] = score;
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if(comp.test(currentVal, nullVal)) {
          nullVal = currentVal;
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


  /*
  * Base strategy for collapsing on a 32 bit numeric field and selecting a group head
  * based on min/max value of a 32 bit numeric field.
  */

  private abstract class IntFieldValueStrategy {
    protected int nullPolicy;
    protected IntIntHashMap cmap;
    protected Scorer scorer;
    protected FloatArrayList nullScores;
    protected float nullScore;
    protected float[] scores;
    protected FixedBitSet collapsedSet;
    protected int nullDoc = -1;
    protected boolean needsScores;
    protected boolean max;
    protected String field;
    protected String collapseField;
    protected int[] docs;
    protected int nullValue;
    protected IntArrayList boostDocs;
    protected IntArrayList boostKeys;
    protected boolean boosts;
    protected MergeBoost mergeBoost;

    public abstract void collapse(int collapseKey, int contextDoc, int globalDoc) throws IOException;
    public abstract void setNextReader(LeafReaderContext context) throws IOException;

    public IntFieldValueStrategy(int maxDoc,
                                 int size,
                                 String collapseField,
                                 String field,
                                 int nullValue,
                                 int nullPolicy,
                                 boolean max,
                                 boolean needsScores,
                                 IntIntHashMap boostDocsMap) {
      this.field = field;
      this.collapseField = collapseField;
      this.nullValue = nullValue;
      this.nullPolicy = nullPolicy;
      this.max = max;
      this.needsScores = needsScores;
      this.collapsedSet = new FixedBitSet(maxDoc);
      this.cmap = new IntIntHashMap(size);
      if(boostDocsMap != null) {
        this.boosts = true;
        this.boostDocs = new IntArrayList();
        this.boostKeys = new IntArrayList();
        int[] bd = new int[boostDocsMap.size()];
        Iterator<IntIntCursor> it =  boostDocsMap.iterator();
        int index = -1;
        while(it.hasNext()) {
          IntIntCursor cursor = it.next();
          bd[++index] = cursor.key;
        }

        Arrays.sort(bd);
        this.mergeBoost = new MergeBoost(bd);
      }
    }

    public FixedBitSet getCollapsedSet() {

      if(nullDoc > -1) {
        this.collapsedSet.set(nullDoc);
      }

      //Handle the boosted docs.
      if(this.boostKeys != null) {
        int s = boostKeys.size();
        for(int i=0; i<s; i++) {
          int key = this.boostKeys.get(i);
          if(key != nullValue) {
            cmap.remove(key);
          }
          //Add the boosted docs to the collapsedSet
          this.collapsedSet.set(boostDocs.get(i));
        }

        mergeBoost.reset();
      }

      Iterator<IntIntCursor> it1 = cmap.iterator();
      while(it1.hasNext()) {
        IntIntCursor cursor = it1.next();
        int pointer = cursor.value;
        collapsedSet.set(docs[pointer]);
      }

      return collapsedSet;
    }

    public void setScorer(Scorer scorer) {
      this.scorer = scorer;
    }

    public FloatArrayList getNullScores() {
      return nullScores;
    }

    public IntIntHashMap getCollapseMap() {
      return cmap;
    }

    public float getNullScore() {
      return this.nullScore;
    }

    public float[] getScores() {
      return scores;
    }

    public int[] getDocs() { return docs;}

    public MergeBoost getMergeBoost()  {
      return this.mergeBoost;
    }
  }

  /*
  *  Strategy for collapsing on a 32 bit numeric field and selecting the group head based
  *  on the min/max value of a 32 bit field numeric field.
  */

  private class IntIntStrategy extends IntFieldValueStrategy {

    private NumericDocValues minMaxVals;
    private int[] testValues;
    private IntCompare comp;
    private int nullCompVal;

    private int index=-1;

    public IntIntStrategy(int maxDoc,
                          int size,
                          String collapseField,
                          String field,
                          int nullValue,
                          int nullPolicy,
                          boolean max,
                          boolean needsScores,
                          IntIntHashMap boostDocs) throws IOException {

      super(maxDoc, size, collapseField, field, nullValue, nullPolicy, max, needsScores, boostDocs);

      this.testValues = new int[size];
      this.docs = new int[size];

      if(max) {
        comp = new MaxIntComp();
        this.nullCompVal = Integer.MIN_VALUE;
      } else {
        comp = new MinIntComp();
        this.nullCompVal = Integer.MAX_VALUE;
      }

      if(needsScores) {
        this.scores = new float[size];
        if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          nullScores = new FloatArrayList();
        }
      }
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.minMaxVals = DocValues.getNumeric(context.reader(), this.field);
    }

    public void collapse(int collapseKey, int contextDoc, int globalDoc) throws IOException {

      // Check to see if we have documents boosted by the QueryElevationComponent
      if(boosts && mergeBoost.boost(globalDoc)) {
        boostDocs.add(globalDoc);
        boostKeys.add(collapseKey);
        return;
      }

      int currentVal = (int) minMaxVals.get(contextDoc);

      if(collapseKey != nullValue) {
        final int idx;
        if((idx = cmap.indexOf(collapseKey)) >= 0) {
          int pointer = cmap.indexGet(idx);
          if(comp.test(currentVal, testValues[pointer])) {
            testValues[pointer]= currentVal;
            docs[pointer] = globalDoc;
            if(needsScores) {
              scores[pointer] = scorer.score();
            }
          }
        } else {
          ++index;
          cmap.put(collapseKey, index);
          if(index == testValues.length) {
            testValues = ArrayUtil.grow(testValues);
            docs = ArrayUtil.grow(docs);
            if(needsScores) {
              scores = ArrayUtil.grow(scores);
            }
          }

          testValues[index] = currentVal;
          docs[index] = (globalDoc);

          if(needsScores) {
            scores[index] = scorer.score();
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if(comp.test(currentVal, nullCompVal)) {
          nullCompVal = currentVal;
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

  private class IntFloatStrategy extends IntFieldValueStrategy {

    private NumericDocValues minMaxVals;
    private float[] testValues;
    private FloatCompare comp;
    private float nullCompVal;

    private int index=-1;

    public IntFloatStrategy(int maxDoc,
                          int size,
                          String collapseField,
                          String field,
                          int nullValue,
                          int nullPolicy,
                          boolean max,
                          boolean needsScores,
                          IntIntHashMap boostDocs) throws IOException {

      super(maxDoc, size, collapseField, field, nullValue, nullPolicy, max, needsScores, boostDocs);

      this.testValues = new float[size];
      this.docs = new int[size];

      if(max) {
        comp = new MaxFloatComp();
        this.nullCompVal = -Float.MAX_VALUE;
      } else {
        comp = new MinFloatComp();
        this.nullCompVal = Float.MAX_VALUE;
      }

      if(needsScores) {
        this.scores = new float[size];
        if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          nullScores = new FloatArrayList();
        }
      }
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.minMaxVals = DocValues.getNumeric(context.reader(), this.field);
    }

    public void collapse(int collapseKey, int contextDoc, int globalDoc) throws IOException {

      // Check to see if we have documents boosted by the QueryElevationComponent
      if(boosts && mergeBoost.boost(globalDoc)) {
        boostDocs.add(globalDoc);
        boostKeys.add(collapseKey);
        return;
      }

      int minMaxVal = (int) minMaxVals.get(contextDoc);
      float currentVal = Float.intBitsToFloat(minMaxVal);

      if(collapseKey != nullValue) {
        final int idx;
        if((idx = cmap.indexOf(collapseKey)) >= 0) {
          int pointer = cmap.indexGet(idx);
          if(comp.test(currentVal, testValues[pointer])) {
            testValues[pointer] = currentVal;
            docs[pointer] = globalDoc;
            if(needsScores) {
              scores[pointer] = scorer.score();
            }
          }
        } else {
          ++index;
          cmap.put(collapseKey, index);
          if(index == testValues.length) {
            testValues = ArrayUtil.grow(testValues);
            docs = ArrayUtil.grow(docs);
            if(needsScores) {
              scores = ArrayUtil.grow(scores);
            }
          }

          testValues[index] = currentVal;
          docs[index] = globalDoc;
          if(needsScores) {
            scores[index] = scorer.score();
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if(comp.test(currentVal, nullCompVal)) {
          nullCompVal = currentVal;
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



  /*
  *  Strategy for collapsing on a 32 bit numeric field and selecting the group head based
  *  on the min/max value of a Value Source Function.
  */

  private class IntValueSourceStrategy extends IntFieldValueStrategy {

    private FloatCompare comp;
    private float[] testValues;
    private float nullCompVal;

    private ValueSource valueSource;
    private FunctionValues functionValues;
    private Map rcontext;
    private CollapseScore collapseScore = new CollapseScore();
    private boolean cscore;
    private float score;
    private int index=-1;

    public IntValueSourceStrategy(int maxDoc,
                                  String funcStr,
                                  int size,
                                  String collapseField,
                                  int nullValue,
                                  int nullPolicy,
                                  boolean max,
                                  boolean needsScores,
                                  IntIntHashMap boostDocs,
                                  FunctionQuery funcQuery,
                                  IndexSearcher searcher) throws IOException {

      super(maxDoc, size, collapseField, null, nullValue, nullPolicy, max, needsScores, boostDocs);

      this.testValues = new float[size];
      this.docs = new int[size];

      this.valueSource = funcQuery.getValueSource();
      this.rcontext = ValueSource.newContext(searcher);

      if(max) {
        this.nullCompVal = -Float.MAX_VALUE;
        comp = new MaxFloatComp();
      } else {
        this.nullCompVal = Float.MAX_VALUE;
        comp = new MinFloatComp();
      }

      if(funcStr.indexOf("cscore()") != -1) {
        this.cscore = true;
        this.rcontext.put("CSCORE",this.collapseScore);
      }

      if(needsScores) {
        this.scores = new float[size];
        if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          nullScores = new FloatArrayList();
        }
      }
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      functionValues = this.valueSource.getValues(rcontext, context);
    }

    public void collapse(int collapseKey, int contextDoc, int globalDoc) throws IOException {

      // Check to see if we have documents boosted by the QueryElevationComponent
      if(boosts && mergeBoost.boost(globalDoc)) {
        boostDocs.add(globalDoc);
        boostKeys.add(collapseKey);
        return;
      }

      if(needsScores || cscore) {
        this.score = scorer.score();
        this.collapseScore.score = score;
      }

      float currentVal = functionValues.floatVal(contextDoc);

      if(collapseKey != nullValue) {
        final int idx;
        if((idx = cmap.indexOf(collapseKey)) >= 0) {
          int pointer = cmap.indexGet(idx);
          if(comp.test(currentVal, testValues[pointer])) {
            testValues[pointer] = currentVal;
            docs[pointer] = globalDoc;
            if(needsScores){
              scores[pointer] = score;
            }
          }
        } else {
          ++index;
          cmap.put(collapseKey, index);
          if(index == testValues.length) {
            testValues = ArrayUtil.grow(testValues);
            docs = ArrayUtil.grow(docs);
            if(needsScores) {
              scores = ArrayUtil.grow(scores);
            }
          }
          docs[index] = globalDoc;
          testValues[index] = currentVal;
          if(needsScores) {
            scores[index] = score;
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if(comp.test(currentVal, nullCompVal)) {
          nullCompVal = currentVal;
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

  static class MergeBoost {

    private int[] boostDocs;
    private int index = 0;

    public MergeBoost(int[] boostDocs) {
      this.boostDocs = boostDocs;
    }

    public void reset() {
      this.index = 0;
    }

    public boolean boost(int globalDoc) {
      if(index == Integer.MIN_VALUE) {
        return false;
      } else {
        while(true) {
          if(index >= boostDocs.length) {
            index = Integer.MIN_VALUE;
            return false;
          } else {
            int comp = boostDocs[index];
            if(comp == globalDoc) {
              ++index;
              return true;
            } else if(comp < globalDoc) {
              ++index;
            } else {
              return false;
            }
          }
        }
      }
    }
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

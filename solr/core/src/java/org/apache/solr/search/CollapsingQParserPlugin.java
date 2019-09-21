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
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.carrotsearch.hppc.FloatArrayList;
import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import com.carrotsearch.hppc.cursors.IntLongCursor;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.QueryElevationComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.uninverting.UninvertingReader;

import static org.apache.solr.common.params.CommonParams.SORT;

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


  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
    return new CollapsingQParser(qstr, localParams, params, request);
  }

  private static class CollapsingQParser extends QParser {

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

  public static enum GroupHeadSelectorType {
    MIN, MAX, SORT, SCORE;
    public static EnumSet<GroupHeadSelectorType> MIN_MAX = EnumSet.of(MIN, MAX);
  }
  
  /**
   * Models all the information about how group head documents should be selected
   */
  public static final class GroupHeadSelector {
    
    /** 
     * The param value for this selector whose meaning depends on type.  
     * (ie: a field or valuesource for MIN/MAX, a sort string for SORT, "score" for SCORE). 
     * Will never be null.
     */
    public final String selectorText;
    /** The type for this selector, will never be null */
    public final GroupHeadSelectorType type;
    private GroupHeadSelector(String s, GroupHeadSelectorType type) {
      assert null != s;
      assert null != type;
      
      this.selectorText = s;
      this.type = type;
    }

    @Override
    public boolean equals(final Object other) {
      if (other instanceof GroupHeadSelector) {
        final GroupHeadSelector that = (GroupHeadSelector) other;
        return (this.type == that.type) && this.selectorText.equals(that.selectorText);
      }
      return false;
    }
    
    @Override
    public int hashCode() {
      return 17 * (31 + selectorText.hashCode()) * (31 + type.hashCode());
    }
    
    /**
     * returns a new GroupHeadSelector based on the specified local params
     */
    public static GroupHeadSelector build(final SolrParams localParams) {
      final String sortString = StringUtils.defaultIfBlank(localParams.get(SORT), null);
      final String max = StringUtils.defaultIfBlank(localParams.get("max"), null);
      final String min = StringUtils.defaultIfBlank(localParams.get("min"), null);

      if (1 < numNotNull(min, max, sortString)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                "At most one localparam for selecting documents (min, max, sort) may be specified: " + localParams.toString());
      }
      
      if (null != sortString) {
        return new GroupHeadSelector(sortString, GroupHeadSelectorType.SORT);
      } else if (null != min) {
        return new GroupHeadSelector(min, GroupHeadSelectorType.MIN);
      } else if (null != max) {
        return new GroupHeadSelector(max, GroupHeadSelectorType.MAX);
      }
      // default
      return new GroupHeadSelector("score", GroupHeadSelectorType.SCORE);
    }
  }

  public static class CollapsingPostFilter extends ExtendedQueryBase implements PostFilter, ScoreFilter {

    private String collapseField;
    private final GroupHeadSelector groupHeadSelector;
    private final SortSpec sortSpec; // may be null, parsed at most once from groupHeadSelector
    public String hint;
    private boolean needsScores = true;
    private boolean needsScores4Collapsing = false;
    private int nullPolicy;
    private Set<BytesRef> boosted; // ordered by "priority"
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

    // Only a subset of fields in hashCode/equals?

    public int hashCode() {
      int hashCode = classHash();
      hashCode = 31 * hashCode + collapseField.hashCode();
      hashCode = 31 * hashCode + groupHeadSelector.hashCode();
      hashCode = 31 * hashCode + nullPolicy;
      return hashCode;
    }

    public boolean equals(Object other) {
      return sameClassAs(other) &&
             equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(CollapsingPostFilter other) {
      return collapseField.equals(other.collapseField) &&
             groupHeadSelector.equals(other.groupHeadSelector) &&
             nullPolicy == other.nullPolicy;
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    public int getCost() {
      return Math.max(super.getCost(), 100);
    }

    public String toString(String s) {
      return s;
    }

    public CollapsingPostFilter(SolrParams localParams, SolrParams params, SolrQueryRequest request) {
      this.collapseField = localParams.get("field");
      if (this.collapseField == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Required 'field' param is missing.");
      }

      // if unknown field, this would fail fast
      SchemaField collapseFieldSf = request.getSchema().getField(this.collapseField);
      if (!(collapseFieldSf.isUninvertible() || collapseFieldSf.hasDocValues())) {
        // uninvertible=false and docvalues=false
        // field can't be indexed=false and uninvertible=true
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collapsing field '" + collapseField +
            "' should be either docValues enabled or indexed with uninvertible enabled");
      } else if (collapseFieldSf.multiValued()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collapsing not supported on multivalued fields");
      }

      this.groupHeadSelector = GroupHeadSelector.build(localParams);
      
      if (groupHeadSelector.type.equals(GroupHeadSelectorType.SORT) &&
          CollapseScore.wantsCScore(groupHeadSelector.selectorText)) {
        // we can't support Sorts that wrap functions that include "cscore()" because
        // the abstraction layer for Sort/SortField rewriting gives each clause it's own
        // context Map which we don't have access to -- so for now, give a useful error
        // (as early as possible) if attempted
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                "Using cscore() as a function in the 'sort' local "+
                                "param of the collapse parser is not supported");
      }

        
      
      this.sortSpec = GroupHeadSelectorType.SORT.equals(groupHeadSelector.type) 
        ? SortSpecParsing.parseSortSpec(groupHeadSelector.selectorText, request)
        : null;

      this.hint = localParams.get("hint");
      this.size = localParams.getInt("size", 100000); //Only used for collapsing on int fields.

      {
        final SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
        assert null != info;
        
        // may be null in some esoteric corner usages
        final ResponseBuilder rb = info.getResponseBuilder();
        final SortSpec topSort = null == rb ? null : rb.getSortSpec();

        this.needsScores4Collapsing = GroupHeadSelectorType.SCORE.equals(groupHeadSelector.type) ||
            (GroupHeadSelectorType.SORT.equals(groupHeadSelector.type)
                && this.sortSpec.includesScore()) ||
            (GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type)
                && CollapseScore.wantsCScore(groupHeadSelector.selectorText));
        this.needsScores = needsScores4Collapsing ||
          (info.getRsp().getReturnFields().wantsScore() ||
           (null != topSort && topSort.includesScore()) ||
           (this.boosted != null));

        if (this.needsScores && null != rb) {
          // regardless of why we need scores ensure the IndexSearcher will compute them
          // for the "real" docs.  (ie: maybe we need them because we were
          // asked to compute them for the collapsed docs, maybe we need them because in
          // order to find the groupHead we need them computed for us.
          
          rb.setFieldFlags( rb.getFieldFlags() | SolrIndexSearcher.GET_SCORES);
        }
      }
      
      String nPolicy = localParams.get("nullPolicy", NULL_IGNORE);
      if(nPolicy.equals(NULL_IGNORE)) {
        this.nullPolicy = NULL_POLICY_IGNORE;
      } else if (nPolicy.equals(NULL_COLLAPSE)) {
        this.nullPolicy = NULL_POLICY_COLLAPSE;
      } else if(nPolicy.equals((NULL_EXPAND))) {
        this.nullPolicy = NULL_POLICY_EXPAND;
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid nullPolicy:"+nPolicy);
      }
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
          this.boosted = (Set<BytesRef>)context.get(QueryElevationComponent.BOOSTED);
        }

        boostDocsMap = QueryElevationComponent.getBoostDocs(searcher, this.boosted, context);
        return collectorFactory.getCollector(this.collapseField,
                                             this.groupHeadSelector,
                                             this.sortSpec,
                                             this.nullPolicy,
                                             this.hint,
                                             this.needsScores4Collapsing,
                                             this.needsScores,
                                             this.size,
                                             boostDocsMap,
                                             searcher);

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

  }

  /**
   * This forces the use of the top level field cache for String fields.
   * This is VERY fast at query time but slower to warm and causes insanity.
   */
  public static LeafReader getTopFieldCacheReader(SolrIndexSearcher searcher, String collapseField) {
    UninvertingReader.Type type = null;
    final SchemaField f = searcher.getSchema().getFieldOrNull(collapseField);
    assert null != f;        // should already be enforced higher up
    assert !f.multiValued(); // should already be enforced higher up
    
    assert f.getType() instanceof StrField; // this method shouldn't be called otherwise
    if (f.indexed() && f.isUninvertible()) {
      type = UninvertingReader.Type.SORTED;
    }
    
    return UninvertingReader.wrap(
        new ReaderWrapper(searcher.getSlowAtomicReader(), collapseField),
        Collections.singletonMap(collapseField, type)::get);
  }

  private static class ReaderWrapper extends FilterLeafReader {

    private final FieldInfos fieldInfos;

    ReaderWrapper(LeafReader leafReader, String field) {
      super(leafReader);

      // TODO can we just do "field" and not bother with the other fields?
      List<FieldInfo> newInfos = new ArrayList<>(in.getFieldInfos().size());
      for (FieldInfo fieldInfo : in.getFieldInfos()) {
        if (fieldInfo.name.equals(field)) {
          FieldInfo f = new FieldInfo(fieldInfo.name,
              fieldInfo.number,
              fieldInfo.hasVectors(),
              fieldInfo.hasNorms(),
              fieldInfo.hasPayloads(),
              fieldInfo.getIndexOptions(),
              DocValuesType.NONE,
              fieldInfo.getDocValuesGen(),
              fieldInfo.attributes(),
              fieldInfo.getPointDataDimensionCount(),
              fieldInfo.getPointIndexDimensionCount(),
              fieldInfo.getPointNumBytes(),
              fieldInfo.isSoftDeletesField());
          newInfos.add(f);
        } else {
          newInfos.add(fieldInfo);
        }
      }
      FieldInfos infos = new FieldInfos(newInfos.toArray(new FieldInfo[newInfos.size()]));
      this.fieldInfos = infos;
    }

    public FieldInfos getFieldInfos() {
      return fieldInfos;
    }

    public SortedDocValues getSortedDocValues(String field) {
      return null;
    }

    // NOTE: delegating the caches is wrong here as we are altering the content
    // of the reader, this should ONLY be used under an uninvertingreader which
    // will restore doc values back using uninversion, otherwise all sorts of
    // crazy things could happen.

    @Override
    public CacheHelper getCoreCacheHelper() {
      return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return in.getReaderCacheHelper();
    }
  }


  private static class ScoreAndDoc extends Scorable {

    public float score;
    public int docId;

    public float score() {
      return score;
    }

    public int docID() {
      return docId;
    }
  }



  /*
  * Collapses on Ordinal Values using Score to select the group head.
  */

  private static class OrdScoreCollector extends DelegatingCollector {

    private LeafReaderContext[] contexts;
    private final DocValuesProducer collapseValuesProducer;
    private FixedBitSet collapsedSet;
    private SortedDocValues collapseValues;
    private OrdinalMap ordinalMap;
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
                             DocValuesProducer collapseValuesProducer,
                             int nullPolicy,
                             IntIntHashMap boostDocsMap) throws IOException {
      this.maxDoc = maxDoc;
      this.contexts = new LeafReaderContext[segments];
      this.collapsedSet = new FixedBitSet(maxDoc);
      this.collapseValuesProducer = collapseValuesProducer;
      this.collapseValues = collapseValuesProducer.getSorted(null);
      
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

    @Override public ScoreMode scoreMode() { return ScoreMode.COMPLETE; }

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
        if (segmentValues.advanceExact(contextDoc)) {
          ord = (int)segmentOrdinalMap.get(segmentValues.ordValue());
        } else {
          ord = -1;
        }
      } else {
        //Handle top Level FieldCache or Single Segment Case
        if (segmentValues.advanceExact(globalDoc)) {
          ord = segmentValues.ordValue();
        } else {
          ord = -1;
        }
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

      collapseValues = collapseValuesProducer.getSorted(null);
      
      if(collapseValues instanceof MultiDocValues.MultiSortedDocValues) {
        this.multiSortedDocValues = (MultiDocValues.MultiSortedDocValues)collapseValues;
        this.ordinalMap = multiSortedDocValues.mapping;
      }

      if(ordinalMap != null) {
        this.segmentValues = this.multiSortedDocValues.values[currentContext];
        this.segmentOrdinalMap = this.ordinalMap.getGlobalOrds(currentContext);
      } else {
        this.segmentValues = collapseValues;
      }

      int nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
      leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
      ScoreAndDoc dummy = new ScoreAndDoc();
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
          if (segmentValues.advanceExact(contextDoc)) {
            ord = (int)segmentOrdinalMap.get(segmentValues.ordValue());
          }
        } else {
          //Handle top Level FieldCache or Single Segment Case
          if (segmentValues.advanceExact(docId)) {
            ord = segmentValues.ordValue();
          }
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

  private static class IntScoreCollector extends DelegatingCollector {

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

    @Override public ScoreMode scoreMode() { return ScoreMode.COMPLETE; }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      this.contexts[context.ord] = context;
      this.docBase = context.docBase;
      this.collapseValues = DocValues.getNumeric(context.reader(), this.field);
    }

    @Override
    public void collect(int contextDoc) throws IOException {
      int collapseValue;
      if (collapseValues.advanceExact(contextDoc)) {
        collapseValue = (int) collapseValues.longValue();
      } else {
        collapseValue = 0;
      }
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

      collapseValues = DocValues.getNumeric(contexts[currentContext].reader(), this.field);
      int nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
      leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
      ScoreAndDoc dummy = new ScoreAndDoc();
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
          collapseValues = DocValues.getNumeric(contexts[currentContext].reader(), this.field);
        }

        int contextDoc = globalDoc-currentDocBase;
        int collapseValue;
        if (collapseValues.advanceExact(contextDoc)) {
          collapseValue = (int) collapseValues.longValue();
        } else {
          collapseValue = 0;
        }

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
  
  /**
   * Collapse on Ordinal value field.
   */
  private static class OrdFieldValueCollector extends DelegatingCollector {
    private LeafReaderContext[] contexts;
    private DocValuesProducer collapseValuesProducer;
    private SortedDocValues collapseValues;
    protected OrdinalMap ordinalMap;
    protected SortedDocValues segmentValues;
    protected LongValues segmentOrdinalMap;
    protected MultiDocValues.MultiSortedDocValues multiSortedDocValues;

    private int maxDoc;
    private int nullPolicy;

    private OrdFieldValueStrategy collapseStrategy;
    private boolean needsScores4Collapsing;
    private boolean needsScores;

    public OrdFieldValueCollector(int maxDoc,
                                  int segments,
                                  DocValuesProducer collapseValuesProducer,
                                  int nullPolicy,
                                  GroupHeadSelector groupHeadSelector,
                                  SortSpec sortSpec,
                                  boolean needsScores4Collapsing,
                                  boolean needsScores,
                                  FieldType fieldType,
                                  IntIntHashMap boostDocs,
                                  FunctionQuery funcQuery, IndexSearcher searcher) throws IOException{

      assert ! GroupHeadSelectorType.SCORE.equals(groupHeadSelector.type);
      
      this.maxDoc = maxDoc;
      this.contexts = new LeafReaderContext[segments];
      this.collapseValuesProducer = collapseValuesProducer;
      this.collapseValues = collapseValuesProducer.getSorted(null);
      if(collapseValues instanceof MultiDocValues.MultiSortedDocValues) {
        this.multiSortedDocValues = (MultiDocValues.MultiSortedDocValues)collapseValues;
        this.ordinalMap = multiSortedDocValues.mapping;
      }

      int valueCount = collapseValues.getValueCount();
      this.nullPolicy = nullPolicy;
      this.needsScores4Collapsing = needsScores4Collapsing;
      this.needsScores = needsScores;
      if (null != sortSpec) {
        this.collapseStrategy = new OrdSortSpecStrategy(maxDoc, nullPolicy, new int[valueCount], groupHeadSelector, this.needsScores4Collapsing, this.needsScores, boostDocs, sortSpec, searcher, collapseValues);
      } else if (funcQuery != null) {
        this.collapseStrategy =  new OrdValueSourceStrategy(maxDoc, nullPolicy, new int[valueCount], groupHeadSelector, this.needsScores4Collapsing, this.needsScores, boostDocs, funcQuery, searcher, collapseValues);
      } else {
        NumberType numType = fieldType.getNumberType();
        if (null == numType) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "min/max must be either Int/Long/Float based field types");
        }
        switch (numType) {
          case INTEGER: {
            this.collapseStrategy = new OrdIntStrategy(maxDoc, nullPolicy, new int[valueCount], groupHeadSelector, this.needsScores, boostDocs, collapseValues);
            break;
          }
          case FLOAT: {
            this.collapseStrategy = new OrdFloatStrategy(maxDoc, nullPolicy, new int[valueCount], groupHeadSelector, this.needsScores, boostDocs, collapseValues);
            break;
          }
          case LONG: {
            this.collapseStrategy =  new OrdLongStrategy(maxDoc, nullPolicy, new int[valueCount], groupHeadSelector, this.needsScores, boostDocs, collapseValues);
            break;
          }
          default: {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "min/max must be either Int/Long/Float field types");
          }
        }
      }
    }

    @Override public ScoreMode scoreMode() { return needsScores ? ScoreMode.COMPLETE : super.scoreMode(); }

    public void setScorer(Scorable scorer) throws IOException {
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
        if (segmentValues.advanceExact(contextDoc)) {
          ord = (int)segmentOrdinalMap.get(segmentValues.ordValue());
        }
      } else {
        if (segmentValues.advanceExact(globalDoc)) {
          ord = segmentValues.ordValue();
        }
      }
      collapseStrategy.collapse(ord, contextDoc, globalDoc);
    }

    public void finish() throws IOException {
      if(contexts.length == 0) {
        return;
      }

      int currentContext = 0;
      int currentDocBase = 0;

      this.collapseValues = collapseValuesProducer.getSorted(null);
      if(collapseValues instanceof MultiDocValues.MultiSortedDocValues) {
        this.multiSortedDocValues = (MultiDocValues.MultiSortedDocValues)collapseValues;
        this.ordinalMap = multiSortedDocValues.mapping;
      }
      if(ordinalMap != null) {
        this.segmentValues = this.multiSortedDocValues.values[currentContext];
        this.segmentOrdinalMap = this.ordinalMap.getGlobalOrds(currentContext);
      } else {
        this.segmentValues = collapseValues;
      }

      int nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
      leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
      ScoreAndDoc dummy = new ScoreAndDoc();
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
            if (segmentValues.advanceExact(contextDoc)) {
              ord = (int) segmentOrdinalMap.get(segmentValues.ordValue());
            }
          } else {
            //Handle top Level FieldCache or Single Segment Case
            if (segmentValues.advanceExact(globalDoc)) {
              ord = segmentValues.ordValue();
            }
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


  /**
   *  Collapses on an integer field.
   */
  private static class IntFieldValueCollector extends DelegatingCollector {
    private LeafReaderContext[] contexts;
    private NumericDocValues collapseValues;
    private int maxDoc;
    private int nullValue;
    private int nullPolicy;

    private IntFieldValueStrategy collapseStrategy;
    private boolean needsScores4Collapsing;
    private boolean needsScores;
    private String collapseField;

    public IntFieldValueCollector(int maxDoc,
                                  int size,
                                  int segments,
                                  int nullValue,
                                  int nullPolicy,
                                  String collapseField,
                                  GroupHeadSelector groupHeadSelector,
                                  SortSpec sortSpec,
                                  boolean needsScores4Collapsing,
                                  boolean needsScores,
                                  FieldType fieldType,
                                  IntIntHashMap boostDocsMap,
                                  FunctionQuery funcQuery,
                                  IndexSearcher searcher) throws IOException{

      assert ! GroupHeadSelectorType.SCORE.equals(groupHeadSelector.type);
      
      this.maxDoc = maxDoc;
      this.contexts = new LeafReaderContext[segments];
      this.collapseField = collapseField;
      this.nullValue = nullValue;
      this.nullPolicy = nullPolicy;
      this.needsScores4Collapsing = needsScores4Collapsing;
      this.needsScores = needsScores;
      if (null != sortSpec) {
        this.collapseStrategy = new IntSortSpecStrategy(maxDoc, size, collapseField, nullValue, nullPolicy, groupHeadSelector, this.needsScores4Collapsing, this.needsScores, boostDocsMap, sortSpec, searcher);
      } else if (funcQuery != null) {
        this.collapseStrategy =  new IntValueSourceStrategy(maxDoc, size, collapseField, nullValue, nullPolicy, groupHeadSelector, this.needsScores4Collapsing, this.needsScores, boostDocsMap, funcQuery, searcher);
      } else {
        NumberType numType = fieldType.getNumberType();
        assert null != numType; // shouldn't make it here for non-numeric types
        switch (numType) {
          case INTEGER: {
            this.collapseStrategy = new IntIntStrategy(maxDoc, size, collapseField, nullValue, nullPolicy, groupHeadSelector, this.needsScores, boostDocsMap);
            break;
          }
          case FLOAT: {
            this.collapseStrategy = new IntFloatStrategy(maxDoc, size, collapseField, nullValue, nullPolicy, groupHeadSelector, this.needsScores, boostDocsMap);
            break;
          }
          default: {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "min/max must be Int or Float field types when collapsing on numeric fields");
          }
        }
      }
    }

    @Override public ScoreMode scoreMode() { return needsScores ? ScoreMode.COMPLETE : super.scoreMode(); }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      this.collapseStrategy.setScorer(scorer);
    }

    public void doSetNextReader(LeafReaderContext context) throws IOException {
      this.contexts[context.ord] = context;
      this.docBase = context.docBase;
      this.collapseStrategy.setNextReader(context);
      this.collapseValues = DocValues.getNumeric(context.reader(), this.collapseField);
    }

    public void collect(int contextDoc) throws IOException {
      int collapseKey;
      if (collapseValues.advanceExact(contextDoc)) {
        collapseKey = (int) collapseValues.longValue();
      } else {
        collapseKey = 0;
      }
      
      int globalDoc = contextDoc+this.docBase;
      collapseStrategy.collapse(collapseKey, contextDoc, globalDoc);
    }

    public void finish() throws IOException {
      if(contexts.length == 0) {
        return;
      }

      int currentContext = 0;
      int currentDocBase = 0;
      this.collapseValues = DocValues.getNumeric(contexts[currentContext].reader(), this.collapseField);
      int nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
      leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
      ScoreAndDoc dummy = new ScoreAndDoc();
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
          this.collapseValues = DocValues.getNumeric(contexts[currentContext].reader(), this.collapseField);
        }

        int contextDoc = globalDoc-currentDocBase;

        if(this.needsScores){
          int collapseValue;
          if (collapseValues.advanceExact(contextDoc)) {
            collapseValue = (int) collapseValues.longValue();
          } else {
            collapseValue = 0;
          }
          
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

  private static class CollectorFactory {
    /** @see #isNumericCollapsible */
    private final static EnumSet<NumberType> NUMERIC_COLLAPSIBLE_TYPES = EnumSet.of(NumberType.INTEGER,
                                                                                    NumberType.FLOAT);
    private boolean isNumericCollapsible(FieldType collapseFieldType) {
      return NUMERIC_COLLAPSIBLE_TYPES.contains(collapseFieldType.getNumberType());
    }

    public DelegatingCollector getCollector(String collapseField,
                                            GroupHeadSelector groupHeadSelector,
                                            SortSpec sortSpec,
                                            int nullPolicy,
                                            String hint,
                                            boolean needsScores4Collapsing,
                                            boolean needsScores,
                                            int size,
                                            IntIntHashMap boostDocs,
                                            SolrIndexSearcher searcher) throws IOException {

      DocValuesProducer docValuesProducer = null;
      FunctionQuery funcQuery = null;

      FieldType collapseFieldType = searcher.getSchema().getField(collapseField).getType();
      String defaultValue = searcher.getSchema().getField(collapseField).getDefaultValue();

      if(collapseFieldType instanceof StrField) {
        if(HINT_TOP_FC.equals(hint)) {
          @SuppressWarnings("resource")
          final LeafReader uninvertingReader = getTopFieldCacheReader(searcher, collapseField);

          docValuesProducer = new EmptyDocValuesProducer() {
              @Override
              public SortedDocValues getSorted(FieldInfo ignored) throws IOException {
                return uninvertingReader.getSortedDocValues(collapseField);
              }
            };
        } else {
          docValuesProducer = new EmptyDocValuesProducer() {
              @Override
              public SortedDocValues getSorted(FieldInfo ignored) throws IOException {
                return DocValues.getSorted(searcher.getSlowAtomicReader(), collapseField);
              }
            };
        }
      } else {
        if(HINT_TOP_FC.equals(hint)) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "top_fc hint is only supported when collapsing on String Fields");
        }
      }

      FieldType minMaxFieldType = null;
      if (GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type)) {
        final String text = groupHeadSelector.selectorText;
        if (text.indexOf("(") == -1) {
          minMaxFieldType = searcher.getSchema().getField(text).getType();
        } else {
          SolrParams params = new ModifiableSolrParams();
          try (SolrQueryRequest request = new LocalSolrQueryRequest(searcher.getCore(), params)) {
            FunctionQParser functionQParser = new FunctionQParser(text, null, null,request);
            funcQuery = (FunctionQuery)functionQParser.parse();
          } catch (SyntaxError e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
          }
        }
      }

      int maxDoc = searcher.maxDoc();
      int leafCount = searcher.getTopReaderContext().leaves().size();

      if (GroupHeadSelectorType.SCORE.equals(groupHeadSelector.type)) {
        
        if (collapseFieldType instanceof StrField) {

          return new OrdScoreCollector(maxDoc, leafCount, docValuesProducer, nullPolicy, boostDocs);

        } else if (isNumericCollapsible(collapseFieldType)) {

          int nullValue = 0;

          // must be non-null at this point
          if (collapseFieldType.getNumberType().equals(NumberType.FLOAT)) {
            if (defaultValue != null) {
              nullValue = Float.floatToIntBits(Float.parseFloat(defaultValue));
            } else {
              nullValue = Float.floatToIntBits(0.0f);
            }
          } else {
            if (defaultValue != null) {
              nullValue = Integer.parseInt(defaultValue);
            }
          }

          return new IntScoreCollector(maxDoc, leafCount, nullValue, nullPolicy, size, collapseField, boostDocs);

        } else {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Collapsing field should be of either String, Int or Float type");
        }
        
      } else { // min, max, sort, etc.. something other then just "score"

        if (collapseFieldType instanceof StrField) {

          return new OrdFieldValueCollector(maxDoc,
                                            leafCount,
                                            docValuesProducer,
                                            nullPolicy,
                                            groupHeadSelector,
                                            sortSpec,
                                            needsScores4Collapsing,
                                            needsScores,
                                            minMaxFieldType,
                                            boostDocs,
                                            funcQuery,
                                            searcher);

        } else if (isNumericCollapsible(collapseFieldType)) {

          int nullValue = 0;

          // must be non-null at this point
          if (collapseFieldType.getNumberType().equals(NumberType.FLOAT)) {
            if (defaultValue != null) {
              nullValue = Float.floatToIntBits(Float.parseFloat(defaultValue));
            } else {
              nullValue = Float.floatToIntBits(0.0f);
            }
          } else {
            if (defaultValue != null) {
              nullValue = Integer.parseInt(defaultValue);
            }
          }

          return new IntFieldValueCollector(maxDoc,
                                            size,
                                            leafCount,
                                            nullValue,
                                            nullPolicy,
                                            collapseField,
                                            groupHeadSelector,
                                            sortSpec,
                                            needsScores4Collapsing,
                                            needsScores,
                                            minMaxFieldType,
                                            boostDocs,
                                            funcQuery,
                                            searcher);
        } else {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Collapsing field should be of either String, Int or Float type");
        }
        
      }
    }
  }

  public static final class CollapseScore {
    /** 
     * Inspects the GroupHeadSelector to determine if this CollapseScore is needed.
     * If it is, then "this" will be added to the readerContext 
     * using the "CSCORE" key, and true will be returned.  If not returns false.
     */
    public boolean setupIfNeeded(final GroupHeadSelector groupHeadSelector,
                                 final Map readerContext) {
      // HACK, but not really any better options until/unless we can recursively
      // ask value sources if they depend on score
      if (wantsCScore(groupHeadSelector.selectorText)) {
        readerContext.put("CSCORE", this);
        return true;
      }
      return false;
    }

    /** 
     * Huge HACK, but not really any better options until/unless we can recursively 
     * ask value sources if they depend on score 
     */
    public static boolean wantsCScore(final String text) {
      return (0 <= text.indexOf("cscore()"));
    }
    
    private CollapseScore() {
      // No-Op
    }
    
    public float score;
  }


  /*
  * Collapse Strategies
  */

  /**
   * The abstract base Strategy for collapse strategies that collapse on an ordinal
   * using min/max field value to select the group head.
   *
   */
  private static abstract class OrdFieldValueStrategy {
    protected int nullPolicy;
    protected int[] ords; 
    protected Scorable scorer;
    protected FloatArrayList nullScores;
    protected float nullScore;
    protected float[] scores;
    protected FixedBitSet collapsedSet;
    protected int nullDoc = -1;
    protected boolean needsScores;
    protected boolean boosts;
    protected IntArrayList boostOrds;
    protected IntArrayList boostDocs;
    protected MergeBoost mergeBoost;
    protected boolean boosted;

    public abstract void collapse(int ord, int contextDoc, int globalDoc) throws IOException;
    public abstract void setNextReader(LeafReaderContext context) throws IOException;

    public OrdFieldValueStrategy(int maxDoc,
                                 int[] ords,
                                 int nullPolicy,
                                 boolean needsScores,
                                 IntIntHashMap boostDocsMap,
                                 SortedDocValues values) {
      this.ords = ords;
      Arrays.fill(ords, -1);
      this.nullPolicy = nullPolicy;
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

    public void setScorer(Scorable scorer) throws IOException {
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
  private static class OrdIntStrategy extends OrdFieldValueStrategy {

    private final String field;
    private NumericDocValues minMaxValues;
    private IntCompare comp;
    private int nullVal;
    private int[] ordVals;

    public OrdIntStrategy(int maxDoc,
                          int nullPolicy,
                          int[] ords,
                          GroupHeadSelector groupHeadSelector,
                          boolean needsScores,
                          IntIntHashMap boostDocs,
                          SortedDocValues values) throws IOException {
      super(maxDoc, ords, nullPolicy, needsScores, boostDocs, values);
      this.field = groupHeadSelector.selectorText;
      this.ordVals = new int[ords.length];

      assert GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type);
      
      if (GroupHeadSelectorType.MAX.equals(groupHeadSelector.type)) {
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

      int currentVal;
      if (minMaxValues.advanceExact(contextDoc)) {
        currentVal = (int) minMaxValues.longValue();
      } else {
        currentVal = 0;
      }
      
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

  /**
   * Strategy for collapsing on ordinal and using the min/max value of a float
   * field to select the group head
   */
  private static class OrdFloatStrategy extends OrdFieldValueStrategy {

    private final String field;
    private NumericDocValues minMaxValues;
    private FloatCompare comp;
    private float nullVal;
    private float[] ordVals;

    public OrdFloatStrategy(int maxDoc,
                            int nullPolicy,
                            int[] ords,
                            GroupHeadSelector groupHeadSelector,
                            boolean needsScores,
                            IntIntHashMap boostDocs,
                            SortedDocValues values) throws IOException {
      super(maxDoc, ords, nullPolicy, needsScores, boostDocs, values);
      this.field = groupHeadSelector.selectorText;
      this.ordVals = new float[ords.length];
      
      assert GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type);

      if (GroupHeadSelectorType.MAX.equals(groupHeadSelector.type)) {
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

      int currentMinMax;
      if (minMaxValues.advanceExact(contextDoc)) {
        currentMinMax = (int) minMaxValues.longValue();
      } else {
        currentMinMax = 0;
      }

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

  private static class OrdLongStrategy extends OrdFieldValueStrategy {

    private final String field;
    private NumericDocValues minMaxVals;
    private LongCompare comp;
    private long nullVal;
    private long[] ordVals;

    public OrdLongStrategy(int maxDoc,
                           int nullPolicy,
                           int[] ords,
                           GroupHeadSelector groupHeadSelector,
                           boolean needsScores,
                           IntIntHashMap boostDocs, SortedDocValues values) throws IOException {
      super(maxDoc, ords, nullPolicy, needsScores, boostDocs, values);
      this.field = groupHeadSelector.selectorText;
      this.ordVals = new long[ords.length];

      assert GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type);
      
      if (GroupHeadSelectorType.MAX.equals(groupHeadSelector.type)) {
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

      long currentVal;
      if (minMaxVals.advanceExact(contextDoc)) {
        currentVal = minMaxVals.longValue();
      } else {
        currentVal = 0;
      }

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
  private static class OrdValueSourceStrategy extends OrdFieldValueStrategy {

    private FloatCompare comp;
    private float nullVal;
    private ValueSource valueSource;
    private FunctionValues functionValues;
    private float[] ordVals;
    private Map rcontext;
    private final CollapseScore collapseScore = new CollapseScore();
    private boolean needsScores4Collapsing;

    public OrdValueSourceStrategy(int maxDoc,
                                  int nullPolicy,
                                  int[] ords,
                                  GroupHeadSelector groupHeadSelector,
                                  boolean needsScores4Collapsing,
                                  boolean needsScores,
                                  IntIntHashMap boostDocs,
                                  FunctionQuery funcQuery,
                                  IndexSearcher searcher,
                                  SortedDocValues values) throws IOException {
      super(maxDoc, ords, nullPolicy, needsScores, boostDocs, values);
      this.needsScores4Collapsing = needsScores4Collapsing;
      this.valueSource = funcQuery.getValueSource();
      this.rcontext = ValueSource.newContext(searcher);
      this.ordVals = new float[ords.length];

      assert GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type);
      
      if (GroupHeadSelectorType.MAX.equals(groupHeadSelector.type)) {
        comp = new MaxFloatComp();
        Arrays.fill(ordVals, -Float.MAX_VALUE );
      } else {
        this.nullVal = Float.MAX_VALUE;
        comp = new MinFloatComp();
        Arrays.fill(ordVals, Float.MAX_VALUE);
      }

      collapseScore.setupIfNeeded(groupHeadSelector, rcontext);

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
      float score = 0;

      if(boosted && mergeBoost.boost(globalDoc)) {
        this.boostOrds.add(ord);
        this.boostDocs.add(globalDoc);
      }

      if (needsScores4Collapsing) {
        score = scorer.score();
        this.collapseScore.score = score;
      }

      float currentVal = functionValues.floatVal(contextDoc);

      if(ord > -1) {
        if(comp.test(currentVal, ordVals[ord])) {
          ords[ord] = globalDoc;
          ordVals[ord] = currentVal;
          if(needsScores) {
            if (!needsScores4Collapsing) {
              score = scorer.score();
            }
            scores[ord] = score;
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if(comp.test(currentVal, nullVal)) {
          nullVal = currentVal;
          nullDoc = globalDoc;
          if(needsScores) {
            if (!needsScores4Collapsing) {
              score = scorer.score();
            }
            nullScore = score;
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        this.collapsedSet.set(globalDoc);
        if(needsScores) {
          if (!needsScores4Collapsing) {
            score = scorer.score();
          }
          nullScores.add(score);
        }
      }
    }
  }

  /*
   * Strategy for collapsing on ordinal and using the first document according to a complex sort
   * as the group head
   */
  private static class OrdSortSpecStrategy extends OrdFieldValueStrategy {

    private final SortFieldsCompare compareState;
    private final Sort sort;

    private float score;
    private boolean needsScores4Collapsing;

    public OrdSortSpecStrategy(int maxDoc,
                               int nullPolicy,
                               int[] ords,
                               GroupHeadSelector groupHeadSelector,
                               boolean needsScores4Collapsing,
                               boolean needsScores,
                               IntIntHashMap boostDocs,
                               SortSpec sortSpec,
                               IndexSearcher searcher,
                               SortedDocValues values) throws IOException {
      super(maxDoc, ords, nullPolicy, needsScores, boostDocs, values);
      this.needsScores4Collapsing = needsScores4Collapsing;
      
      assert GroupHeadSelectorType.SORT.equals(groupHeadSelector.type);
      
      this.sort = rewriteSort(sortSpec, searcher);
      
      this.compareState = new SortFieldsCompare(sort.getSort(), ords.length);

      if (this.needsScores) {
        this.scores = new float[ords.length];
        if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          nullScores = new FloatArrayList();
        }
      }
    }

    @Override
    public void setNextReader(LeafReaderContext context) throws IOException {
      compareState.setNextReader(context);
    }

    @Override
    public void setScorer(Scorable s) throws IOException {
      super.setScorer(s);
      this.compareState.setScorer(s);
    }
    
    @Override
    public void collapse(int ord, int contextDoc, int globalDoc) throws IOException {
      
      if(boosted && mergeBoost.boost(globalDoc)) {
        this.boostOrds.add(ord);
        this.boostDocs.add(globalDoc);
      }

      if (needsScores4Collapsing) {
        this.score = scorer.score();
      }

      if (ord > -1) { // real collapseKey
        if (-1 == ords[ord]) {
          // we've never seen this ord (aka: collapseKey) before, treat it as group head for now
          compareState.setGroupValues(ord, contextDoc);
          ords[ord] = globalDoc;
          if (needsScores) {
            if (!needsScores4Collapsing) {
              this.score = scorer.score();
            }
            scores[ord] = score;
          }
        } else {
          // test this ord to see if it's a new group leader
          if (compareState.testAndSetGroupValues(ord, contextDoc)) {//TODO X
            ords[ord] = globalDoc;
            if (needsScores) {
              if (!needsScores4Collapsing) {
                this.score = scorer.score();
              }
              scores[ord] = score;
            }
          }
        }
      } else if (this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if (-1 == nullDoc) {
          // we've never seen a doc with null collapse key yet, treat it as the null group head for now
          compareState.setNullGroupValues(contextDoc);
          nullDoc = globalDoc;
          if (needsScores) {
            if (!needsScores4Collapsing) {
              this.score = scorer.score();
            }
            nullScore = score;
          }
        } else {
          // test this doc to see if it's the new null leader
          if (compareState.testAndSetNullGroupValues(contextDoc)) {
            nullDoc = globalDoc;
            if (needsScores) {
              if (!needsScores4Collapsing) {
                this.score = scorer.score();
              }
              nullScore = score;
            }
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        this.collapsedSet.set(globalDoc);
        if (needsScores) {
          if (!needsScores4Collapsing) {
            this.score = scorer.score();
          }
          nullScores.add(score);
        }
      }
    }
  }

  /*
  * Base strategy for collapsing on a 32 bit numeric field and selecting a group head
  * based on min/max value of a 32 bit numeric field.
  */

  private static abstract class IntFieldValueStrategy {
    protected int nullPolicy;
    protected IntIntHashMap cmap;
    protected Scorable scorer;
    protected FloatArrayList nullScores;
    protected float nullScore;
    protected float[] scores;
    protected FixedBitSet collapsedSet;
    protected int nullDoc = -1;
    protected boolean needsScores;
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
                                 int nullValue,
                                 int nullPolicy,
                                 boolean needsScores,
                                 IntIntHashMap boostDocsMap) {
      this.collapseField = collapseField;
      this.nullValue = nullValue;
      this.nullPolicy = nullPolicy;
      this.needsScores = needsScores;
      this.collapsedSet = new FixedBitSet(maxDoc);
      this.cmap = new IntIntHashMap(size);
      this.docs = new int[size];
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

    public void setScorer(Scorable scorer) throws IOException {
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
  private static class IntIntStrategy extends IntFieldValueStrategy {

    private final String field;
    private NumericDocValues minMaxVals;
    private int[] testValues;
    private IntCompare comp;
    private int nullCompVal;

    private int index=-1;

    public IntIntStrategy(int maxDoc,
                          int size,
                          String collapseField,
                          int nullValue,
                          int nullPolicy,
                          GroupHeadSelector groupHeadSelector,
                          boolean needsScores,
                          IntIntHashMap boostDocs) throws IOException {

      super(maxDoc, size, collapseField, nullValue, nullPolicy, needsScores, boostDocs);
      this.field = groupHeadSelector.selectorText;
      this.testValues = new int[size];

      assert GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type);
      
      if (GroupHeadSelectorType.MAX.equals(groupHeadSelector.type)) {
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

      int currentVal;
      if (minMaxVals.advanceExact(contextDoc)) {
        currentVal = (int) minMaxVals.longValue();
      } else {
        currentVal = 0;
      }

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

  private static class IntFloatStrategy extends IntFieldValueStrategy {

    private final String field;
    private NumericDocValues minMaxVals;
    private float[] testValues;
    private FloatCompare comp;
    private float nullCompVal;

    private int index=-1;

    public IntFloatStrategy(int maxDoc,
                            int size,
                            String collapseField,
                            int nullValue,
                            int nullPolicy,
                            GroupHeadSelector groupHeadSelector,
                            boolean needsScores,
                            IntIntHashMap boostDocs) throws IOException {

      super(maxDoc, size, collapseField, nullValue, nullPolicy, needsScores, boostDocs);
      this.field = groupHeadSelector.selectorText;
      this.testValues = new float[size];

      assert GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type);
      
      if (GroupHeadSelectorType.MAX.equals(groupHeadSelector.type)) {
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

      int minMaxVal;
      if (minMaxVals.advanceExact(contextDoc)) {
        minMaxVal = (int) minMaxVals.longValue();
      } else {
        minMaxVal = 0;
      }

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
  private static class IntValueSourceStrategy extends IntFieldValueStrategy {

    private FloatCompare comp;
    private float[] testValues;
    private float nullCompVal;

    private ValueSource valueSource;
    private FunctionValues functionValues;
    private Map rcontext;
    private final CollapseScore collapseScore = new CollapseScore();
    private int index=-1;
    private boolean needsScores4Collapsing;

    public IntValueSourceStrategy(int maxDoc,
                                  int size,
                                  String collapseField,
                                  int nullValue,
                                  int nullPolicy,
                                  GroupHeadSelector groupHeadSelector,
                                  boolean needsScores4Collapsing,
                                  boolean needsScores,
                                  IntIntHashMap boostDocs,
                                  FunctionQuery funcQuery,
                                  IndexSearcher searcher) throws IOException {

      super(maxDoc, size, collapseField, nullValue, nullPolicy, needsScores, boostDocs);

      this.needsScores4Collapsing = needsScores4Collapsing;
      this.testValues = new float[size];

      this.valueSource = funcQuery.getValueSource();
      this.rcontext = ValueSource.newContext(searcher);

      assert GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type);
      
      if (GroupHeadSelectorType.MAX.equals(groupHeadSelector.type)) {
        this.nullCompVal = -Float.MAX_VALUE;
        comp = new MaxFloatComp();
      } else {
        this.nullCompVal = Float.MAX_VALUE;
        comp = new MinFloatComp();
      }

      collapseScore.setupIfNeeded(groupHeadSelector, rcontext);

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
      float score = 0;

      // Check to see if we have documents boosted by the QueryElevationComponent
      if(boosts && mergeBoost.boost(globalDoc)) {
        boostDocs.add(globalDoc);
        boostKeys.add(collapseKey);
        return;
      }

      if (needsScores4Collapsing) {
        score = scorer.score();
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
              if (!needsScores4Collapsing) {
                score = scorer.score();
              }
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
            if (!needsScores4Collapsing) {
              score = scorer.score();
            }
            scores[index] = score;
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if(comp.test(currentVal, nullCompVal)) {
          nullCompVal = currentVal;
          nullDoc = globalDoc;
          if(needsScores) {
            if (!needsScores4Collapsing) {
              score = scorer.score();
            }
            nullScore = score;
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        this.collapsedSet.set(globalDoc);
        if(needsScores) {
          if (!needsScores4Collapsing) {
            score = scorer.score();
          }
          nullScores.add(score);
        }
      }
    }
  }

  
  /*
   * Strategy for collapsing on a 32 bit numeric field and using the first document according 
   * to a complex sort as the group head
   */
  private static class IntSortSpecStrategy extends IntFieldValueStrategy {

    private final SortFieldsCompare compareState;
    private final SortSpec sortSpec;
    private final Sort sort;
    
    private int index=-1;
    private boolean needsScores4Collapsing;

    public IntSortSpecStrategy(int maxDoc,
                               int size,
                               String collapseField,
                               int nullValue,
                               int nullPolicy,
                               GroupHeadSelector groupHeadSelector,
                               boolean needsScores4Collapsing,
                               boolean needsScores,
                               IntIntHashMap boostDocs,
                               SortSpec sortSpec,
                               IndexSearcher searcher) throws IOException {
      
      super(maxDoc, size, collapseField, nullValue, nullPolicy, needsScores, boostDocs);
      this.needsScores4Collapsing = needsScores4Collapsing;

      assert GroupHeadSelectorType.SORT.equals(groupHeadSelector.type);

      this.sortSpec = sortSpec;
      this.sort = rewriteSort(sortSpec, searcher);
      this.compareState = new SortFieldsCompare(sort.getSort(), size);

      if(needsScores) {
        this.scores = new float[size];
        if(nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
          nullScores = new FloatArrayList();
        }
      }
    }

    @Override
    public void setNextReader(LeafReaderContext context) throws IOException {
      compareState.setNextReader(context);
    }

    @Override
    public void setScorer(Scorable s) throws IOException {
      super.setScorer(s);
      this.compareState.setScorer(s);
    }

    public void collapse(int collapseKey, int contextDoc, int globalDoc) throws IOException {
      float score = 0;

      // Check to see if we have documents boosted by the QueryElevationComponent
      if(boosts && mergeBoost.boost(globalDoc)) {
        boostDocs.add(globalDoc);
        boostKeys.add(collapseKey);
        return;
      }

      if (needsScores4Collapsing) {
        score = scorer.score();
      }

      if (collapseKey != nullValue) {
        final int idx;
        if ((idx = cmap.indexOf(collapseKey)) >= 0) {
          // we've seen this collapseKey before, test to see if it's a new group leader
          int pointer = cmap.indexGet(idx);
          if (compareState.testAndSetGroupValues(pointer, contextDoc)) {
            docs[pointer] = globalDoc;
            if (needsScores) {
              if (!needsScores4Collapsing) {
                score = scorer.score();
              }
              scores[pointer] = score;
            }
          }
        } else {
          // we've never seen this collapseKey before, treat it as group head for now
          ++index;
          cmap.put(collapseKey, index);
          if (index == docs.length) {
            docs = ArrayUtil.grow(docs);
            compareState.grow(docs.length);
            if(needsScores) {
              scores = ArrayUtil.grow(scores);
            }
          }
          docs[index] = globalDoc;
          compareState.setGroupValues(index, contextDoc);
          if(needsScores) {
            if (!needsScores4Collapsing) {
              score = scorer.score();
            }
            scores[index] = score;
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_COLLAPSE) {
        if (-1 == nullDoc) {
          // we've never seen a doc with null collapse key yet, treat it as the null group head for now
          compareState.setNullGroupValues(contextDoc);
          nullDoc = globalDoc;
          if (needsScores) {
            if (!needsScores4Collapsing) {
              score = scorer.score();
            }
            nullScore = score;
          }
        } else {
          // test this doc to see if it's the new null leader
          if (compareState.testAndSetNullGroupValues(contextDoc)) {
            nullDoc = globalDoc;
            if (needsScores) {
              if (!needsScores4Collapsing) {
                score = scorer.score();
              }
              nullScore = score;
            }
          }
        }
      } else if(this.nullPolicy == CollapsingPostFilter.NULL_POLICY_EXPAND) {
        this.collapsedSet.set(globalDoc);
        if (needsScores) {
          if (!needsScores4Collapsing) {
            score = scorer.score();
          }
          nullScores.add(score);
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

  /** 
   * Class for comparing documents according to a list of SortField clauses and 
   * tracking the groupHeadLeaders and their sort values.  groups will be identified 
   * by int "contextKey values, which may either be (encoded) 32bit numeric values, or 
   * ordinal values for Strings -- this class doesn't care, and doesn't assume any special 
   * meaning.
   */
  private static class SortFieldsCompare {
    final private int numClauses;
    final private SortField[] sorts;
    final private int[] reverseMul;
    final private FieldComparator[] fieldComparators;
    final private LeafFieldComparator[] leafFieldComparators;

    private Object[][] groupHeadValues; // growable
    final private Object[] nullGroupValues;
    
    /**
     * Constructs an instance based on the the (raw, un-rewritten) SortFields to be used, 
     * and an initial number of expected groups (will grow as needed).
     */
    public SortFieldsCompare(SortField[] sorts, int initNumGroups) {
      this.sorts = sorts;
      numClauses = sorts.length;
      fieldComparators = new FieldComparator[numClauses];
      leafFieldComparators = new LeafFieldComparator[numClauses];
      reverseMul = new int[numClauses];
      for (int clause = 0; clause < numClauses; clause++) {
        SortField sf = sorts[clause];
        // we only need one slot for every comparator
        fieldComparators[clause] = sf.getComparator(1, clause);
        reverseMul[clause] = sf.getReverse() ? -1 : 1;
      }
      groupHeadValues = new Object[initNumGroups][];
      nullGroupValues = new Object[numClauses];
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      for (int clause = 0; clause < numClauses; clause++) {
        leafFieldComparators[clause] = fieldComparators[clause].getLeafComparator(context);
      }
    }
    public void setScorer(Scorable s) throws IOException {
      for (int clause = 0; clause < numClauses; clause++) {
        leafFieldComparators[clause].setScorer(s);
      }
    }
    
    // LUCENE-6808 workaround
    private static Object cloneIfBytesRef(Object val) {
      if (val instanceof BytesRef) {
        return BytesRef.deepCopyOf((BytesRef) val);
      }
      return val;
    }

    /**
     * Returns the current SortField values for the specified collapseKey.  
     * If this collapseKey has never been seen before, then an array of null values is inited 
     * and tracked so that the caller may update it if needed.
     */
    private Object[] getOrInitGroupHeadValues(int collapseKey) {
      Object[] values = groupHeadValues[collapseKey];
      if (null == values) {
        values = new Object[numClauses];
        groupHeadValues[collapseKey] = values;
      }
      return values;
    }
      
    /**
     * Records the SortField values for the specified contextDoc as the "best" values 
     * for the group identified by the specified collapseKey. 
     *
     * Should be called the first time a contextKey is encountered.
     */
    public void setGroupValues(int collapseKey, int contextDoc) throws IOException {
      assert 0 <= collapseKey : "negative collapseKey";
      assert collapseKey < groupHeadValues.length : "collapseKey too big -- need to grow array?";
      setGroupValues(getOrInitGroupHeadValues(collapseKey), contextDoc);
    }
    
    /**
     * Records the SortField values for the specified contextDoc as the "best" values 
     * for the null group.
     *
     * Should be calledthe first time a doc in the null group is encountered
     */
    public void setNullGroupValues(int contextDoc) throws IOException {
      setGroupValues(nullGroupValues, contextDoc);
    }
    
    /**
     * Records the SortField values for the specified contextDoc into the 
     * values array provided by the caller.
     */
    private void setGroupValues(Object[] values, int contextDoc) throws IOException {
      for (int clause = 0; clause < numClauses; clause++) {
        leafFieldComparators[clause].copy(0, contextDoc);
        values[clause] = cloneIfBytesRef(fieldComparators[clause].value(0));
      }
    }

    /**
     * Compares the SortField values of the specified contextDoc with the existing group head 
     * values for the group identified by the specified collapseKey, and overwrites them
     * (and returns true) if this document should become the new group head in accordance 
     * with the SortFields
     * (otherwise returns false)
     */
    public boolean testAndSetGroupValues(int collapseKey, int contextDoc) throws IOException {
      assert 0 <= collapseKey : "negative collapseKey";
      assert collapseKey < groupHeadValues.length : "collapseKey too big -- need to grow array?";
      return testAndSetGroupValues(getOrInitGroupHeadValues(collapseKey), contextDoc);
    }
    
    /**
     * Compares the SortField values of the specified contextDoc with the existing group head 
     * values for the null group, and overwrites them (and returns true) if this document 
     * should become the new group head in accordance with the SortFields. 
     * (otherwise returns false)
     */
    public boolean testAndSetNullGroupValues(int contextDoc) throws IOException {
      return testAndSetGroupValues(nullGroupValues, contextDoc);
    }

    /**
     * Compares the SortField values of the specified contextDoc with the existing values
     * array, and overwrites them (and returns true) if this document is the new group head in 
     * accordance with the SortFields.
     * (otherwise returns false)
     */
    private boolean testAndSetGroupValues(Object[] values, int contextDoc) throws IOException {
      Object[] stash = new Object[numClauses];
      int lastCompare = 0;
      int testClause = 0;
      for (/* testClause */; testClause < numClauses; testClause++) {
        leafFieldComparators[testClause].copy(0, contextDoc);
        FieldComparator fcomp = fieldComparators[testClause];
        stash[testClause] = cloneIfBytesRef(fcomp.value(0));
        lastCompare = reverseMul[testClause] * fcomp.compareValues(stash[testClause], values[testClause]);
        
        if (0 != lastCompare) {
          // no need to keep checking additional clauses
          break;
        }
      }

      if (0 <= lastCompare) {
        // we're either not competitive, or we're completely tied with another doc that's already group head
        // that's already been selected
        return false;
      } // else...
      
      // this doc is our new group head, we've already read some of the values into our stash
      testClause++;
      System.arraycopy(stash, 0, values, 0, testClause);
      // read the remaining values we didn't need to test
      for (int copyClause = testClause; copyClause < numClauses; copyClause++) {
        leafFieldComparators[copyClause].copy(0, contextDoc);
        values[copyClause] = cloneIfBytesRef(fieldComparators[copyClause].value(0));
      }
      return true;
    }

    /**
     * Grows all internal arrays to the specified minSize
     */
    public void grow(int minSize) {
      groupHeadValues = ArrayUtil.grow(groupHeadValues, minSize);
    }
  }
    
  private static interface IntCompare {
    public boolean test(int i1, int i2);
  }

  private static interface FloatCompare {
    public boolean test(float i1, float i2);
  }

  private static interface LongCompare {
    public boolean test(long i1, long i2);
  }

  private static class MaxIntComp implements IntCompare {
    public boolean test(int i1, int i2) {
      return i1 > i2;
    }
  }

  private static class MinIntComp implements IntCompare {
    public boolean test(int i1, int i2) {
      return i1 < i2;
    }
  }

  private static class MaxFloatComp implements FloatCompare {
    public boolean test(float i1, float i2) {
      return i1 > i2;
    }
  }

  private static class MinFloatComp implements FloatCompare {
    public boolean test(float i1, float i2) {
      return i1 < i2;
    }
  }

  private static class MaxLongComp implements LongCompare {
    public boolean test(long i1, long i2) {
      return i1 > i2;
    }
  }

  private static class MinLongComp implements LongCompare {
    public boolean test(long i1, long i2) {
      return i1 < i2;
    }
  }

  /** returns the number of arguments that are non null */
  private static final int numNotNull(final Object... args) {
    int r = 0;
    for (final Object o : args) {
      if (null != o) {
        r++;
      }
    }
    return r;
  }

  /**
   * Helper method for rewriting the Sort associated with a SortSpec.  
   * Handles the special case default of relevancy sort (ie: a SortSpec w/null Sort object)
   */
  public static Sort rewriteSort(SortSpec sortSpec, IndexSearcher searcher) throws IOException {
    assert null != sortSpec : "SortSpec must not be null";
    assert null != searcher : "Searcher must not be null";
    Sort orig = sortSpec.getSort();
    if (null == orig) {
      orig = Sort.RELEVANCE;
    }
    return orig.rewrite(searcher);
  }
}

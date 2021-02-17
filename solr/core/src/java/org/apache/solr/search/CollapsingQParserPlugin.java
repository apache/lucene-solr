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
import java.lang.invoke.MethodHandles;
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
import com.carrotsearch.hppc.procedures.IntProcedure;
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
import org.apache.solr.common.params.GroupParams;
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
import org.apache.solr.util.IntFloatDynamicMap;
import org.apache.solr.util.IntIntDynamicMap;
import org.apache.solr.util.IntLongDynamicMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String NAME = "collapse";
  public static final String HINT_TOP_FC = "top_fc";
  
  /**
   * <p>
   * Indicates that values in the collapse field are unique per contiguous block, and a single pass "block based" 
   * collapse algorithm can be used.  This behavior is the default for collapsing on the <code>_root_</code> field,
   * but may also be enabled for other fields that have the same characteristics.  This hint will be ignored if 
   * other options prevent the use of this single pass approach (notable: nullPolicy=collapse)
   * </p>
   * <p>
   * <em>Do <strong>NOT</strong> use this hint if the index is not laid out such that each unique value in the 
   * collapse field is garuntteed to only exist in one contiguous block, otherwise the results of the collapse 
   * filter will include more then one document per collapse value.</em>
   * </p>
   */
  public static final String HINT_BLOCK = "block";

  /**
   * If elevation is used in combination with the collapse query parser, we can define that we only want to return the
   * representative and not all elevated docs by setting this parameter to false (true by default).
   */
  public static String COLLECT_ELEVATED_DOCS_WHEN_COLLAPSING = "collectElevatedDocsWhenCollapsing";

  /**
   * @deprecated use {@link NullPolicy} instead.
   */
  @Deprecated
  public static final String NULL_COLLAPSE = "collapse";
  @Deprecated
  public static final String NULL_IGNORE = "ignore";
  @Deprecated
  public static final String NULL_EXPAND = "expand";
  @Deprecated
  public static final String HINT_MULTI_DOCVALUES = "multi_docvalues";

  public enum NullPolicy {
    IGNORE("ignore", 0),
    COLLAPSE("collapse", 1),
    EXPAND("expand", 2);

    private final String name;
    private final int code;

    NullPolicy(String name, int code) {
      this.name = name;
      this.code = code;
    }

    public String getName() {
      return name;
    }

    public int getCode() {
      return code;
    }

    public static NullPolicy fromString(String nullPolicy) {
      if (StringUtils.isEmpty(nullPolicy)) {
        return DEFAULT_POLICY;
      }
      switch (nullPolicy) {
        case "ignore": return IGNORE;
        case "collapse": return COLLAPSE;
        case "expand": return EXPAND;
        default:
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid nullPolicy: " + nullPolicy);
      }
    }

    static NullPolicy DEFAULT_POLICY = IGNORE;
  }

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

    @Override
    public String toString(){
      return "GroupHeadSelector(selectorText=" + this.selectorText + ", type=" +this.type + ")";
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

  public static class CollapsingPostFilter extends ExtendedQueryBase implements PostFilter {

    private String collapseField;
    private final GroupHeadSelector groupHeadSelector;
    private final SortSpec sortSpec; // may be null, parsed at most once from groupHeadSelector
    public String hint;
    private boolean needsScores = true;
    private boolean needsScores4Collapsing = false;
    private NullPolicy nullPolicy;
    private Set<BytesRef> boosted; // ordered by "priority"
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
      hashCode = 31 * hashCode + nullPolicy.hashCode();
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
      return "CollapsingPostFilter(field=" + this.collapseField +
          ", nullPolicy=" + this.nullPolicy.getName() + ", " +
          this.groupHeadSelector +
          (hint == null ? "": ", hint=" + this.hint) +
          ", size=" + this.size
          + ")";
    }

    public CollapsingPostFilter(SolrParams localParams, SolrParams params, SolrQueryRequest request) {
      // Don't allow collapsing if grouping is being used.
      if (request.getParams().getBool(GroupParams.GROUP, false)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can not use collapse with Grouping enabled");
      }

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

      this.nullPolicy = NullPolicy.fromString(localParams.get("nullPolicy"));
    }

    @SuppressWarnings({"unchecked"})
    public DelegatingCollector getFilterCollector(IndexSearcher indexSearcher) {
      try {

        SolrIndexSearcher searcher = (SolrIndexSearcher)indexSearcher;
        CollectorFactory collectorFactory = new CollectorFactory();
        //Deal with boosted docs.
        //We have to deal with it here rather then the constructor because
        //because the QueryElevationComponent runs after the Queries are constructed.

        IntIntHashMap boostDocsMap = null;
        @SuppressWarnings({"rawtypes"})
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
                                             this.nullPolicy.getCode(),
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
              fieldInfo.getPointDimensionCount(),
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

  /**
   * Collapses on Ordinal Values using Score to select the group head.
   * @lucene.internal
   */
  static class OrdScoreCollector extends DelegatingCollector {

    private LeafReaderContext[] contexts;
    private final DocValuesProducer collapseValuesProducer;
    private FixedBitSet collapsedSet;
    private SortedDocValues collapseValues;
    private OrdinalMap ordinalMap;
    private SortedDocValues segmentValues;
    private LongValues segmentOrdinalMap;
    private MultiDocValues.MultiSortedDocValues multiSortedDocValues;
    private IntIntDynamicMap ords;
    private IntFloatDynamicMap scores;
    private int maxDoc;
    private int nullPolicy;
    private float nullScore = -Float.MAX_VALUE;
    private int nullDoc = -1;
    private boolean collectElevatedDocsWhenCollapsing;
    private FloatArrayList nullScores;

    private final BoostedDocsCollector boostedDocsCollector;

    public OrdScoreCollector(int maxDoc,
                             int segments,
                             DocValuesProducer collapseValuesProducer,
                             int nullPolicy,
                             IntIntHashMap boostDocsMap,
                             IndexSearcher searcher,
                             boolean collectElevatedDocsWhenCollapsing) throws IOException {
      this.maxDoc = maxDoc;
      this.contexts = new LeafReaderContext[segments];
      this.collectElevatedDocsWhenCollapsing = collectElevatedDocsWhenCollapsing;
      List<LeafReaderContext> con = searcher.getTopReaderContext().leaves();
      for(int i=0; i<con.size(); i++) {
        contexts[i] = con.get(i);
      }

      this.collapsedSet = new FixedBitSet(maxDoc);
      this.collapseValuesProducer = collapseValuesProducer;
      this.collapseValues = collapseValuesProducer.getSorted(null);

      int valueCount = collapseValues.getValueCount();
      if(collapseValues instanceof MultiDocValues.MultiSortedDocValues) {
        this.multiSortedDocValues = (MultiDocValues.MultiSortedDocValues)collapseValues;
        this.ordinalMap = multiSortedDocValues.mapping;
      }
      this.ords = new IntIntDynamicMap(valueCount, -1);
      this.scores = new IntFloatDynamicMap(valueCount, -Float.MAX_VALUE);
      this.nullPolicy = nullPolicy;
      if(nullPolicy == NullPolicy.EXPAND.getCode()) {
        nullScores = new FloatArrayList();
      }
      this.boostedDocsCollector = BoostedDocsCollector.build(boostDocsMap);
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

      if (collectElevatedDocsWhenCollapsing) {
        // Check to see if we have documents boosted by the QueryElevationComponent
        if (0 <= ord) {
          if (boostedDocsCollector.collectIfBoosted(ord, globalDoc)) return;
        } else {
          if (boostedDocsCollector.collectInNullGroupIfBoosted(globalDoc)) return;
        }
      }

      if(ord > -1) {
        float score = scorer.score();
        if(score > scores.get(ord)) {
          ords.put(ord, globalDoc);
          scores.put(ord, score);
        }
      } else if(nullPolicy == NullPolicy.COLLAPSE.getCode()) {
        float score = scorer.score();
        if(score > nullScore) {
          nullScore = score;
          nullDoc = globalDoc;
        }
      } else if(nullPolicy == NullPolicy.EXPAND.getCode()) {
        collapsedSet.set(globalDoc);
        nullScores.add(scorer.score());
      }
    }

    @Override
    public void finish() throws IOException {
      if(contexts.length == 0) {
        return;
      }

      // Handle the boosted docs.
      boostedDocsCollector.purgeGroupsThatHaveBoostedDocs(collapsedSet,
                                                          (ord) -> { ords.remove(ord); },
                                                          () -> { nullDoc = -1; });

      //Build the sorted DocSet of group heads.
      if(nullDoc > -1) {
        collapsedSet.set(nullDoc);
      }
      ords.forEachValue(doc -> collapsedSet.set(doc));

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
      final MergeBoost mergeBoost = boostedDocsCollector.getMergeBoost();
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
          dummy.score = scores.get(ord);
        } else if(mergeBoost.boost(docId)) {
          //Ignore so it doesn't mess up the null scoring.
        } else if(this.nullPolicy == NullPolicy.COLLAPSE.getCode()) {
          dummy.score = nullScore;
        } else if(this.nullPolicy == NullPolicy.EXPAND.getCode()) {
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

  /**
   * Collapses on an integer field using the score to select the group head.
   * @lucene.internal
   */
  static class IntScoreCollector extends DelegatingCollector {

    private LeafReaderContext[] contexts;
    private FixedBitSet collapsedSet;
    private NumericDocValues collapseValues;
    private IntLongHashMap cmap;
    private int maxDoc;
    private int nullPolicy;
    private float nullScore = -Float.MAX_VALUE;
    private int nullDoc = -1;
    private FloatArrayList nullScores;
    private String field;
    private boolean collectElevatedDocsWhenCollapsing;

    private final BoostedDocsCollector boostedDocsCollector;
    
    public IntScoreCollector(int maxDoc,
                             int segments,
                             int nullPolicy,
                             int size,
                             String field,
                             IntIntHashMap boostDocsMap,
                             IndexSearcher searcher,
                             boolean collectElevatedDocsWhenCollapsing) {
      this.maxDoc = maxDoc;
      this.contexts = new LeafReaderContext[segments];
      this.collectElevatedDocsWhenCollapsing = collectElevatedDocsWhenCollapsing;
      List<LeafReaderContext> con = searcher.getTopReaderContext().leaves();
      for(int i=0; i<con.size(); i++) {
        contexts[i] = con.get(i);
      }

      this.collapsedSet = new FixedBitSet(maxDoc);
      this.nullPolicy = nullPolicy;
      if(nullPolicy == NullPolicy.EXPAND.getCode()) {
        nullScores = new FloatArrayList();
      }
      this.cmap = new IntLongHashMap(size);
      this.field = field;
      
      this.boostedDocsCollector = BoostedDocsCollector.build(boostDocsMap);
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
      final int globalDoc = docBase+contextDoc;
      if (collapseValues.advanceExact(contextDoc)) {
        final int collapseValue = (int) collapseValues.longValue();

        if (collectElevatedDocsWhenCollapsing) {
          // Check to see if we have documents boosted by the QueryElevationComponent (skip normal strategy based collection)
          if (boostedDocsCollector.collectIfBoosted(collapseValue, globalDoc)) return;
        }

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

      } else { // Null Group...

        if (collectElevatedDocsWhenCollapsing){
          // Check to see if we have documents boosted by the QueryElevationComponent (skip normal strategy based collection)
          if (boostedDocsCollector.collectInNullGroupIfBoosted(globalDoc)) return;
        }

        if(nullPolicy == NullPolicy.COLLAPSE.getCode()) {
          float score = scorer.score();
          if(score > this.nullScore) {
            this.nullScore = score;
            this.nullDoc = globalDoc;
          }
        } else if(nullPolicy == NullPolicy.EXPAND.getCode()) {
          collapsedSet.set(globalDoc);
          nullScores.add(scorer.score());
        }
      }

    }

    @Override
    public void finish() throws IOException {
      if(contexts.length == 0) {
        return;
      }

      // Handle the boosted docs.
      boostedDocsCollector.purgeGroupsThatHaveBoostedDocs(collapsedSet,
                                                          (key) -> { cmap.remove(key); },
                                                          () -> { nullDoc = -1; });

      //Build the sorted DocSet of group heads.
      if(nullDoc > -1) {
        collapsedSet.set(nullDoc);
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
      final MergeBoost mergeBoost = boostedDocsCollector.getMergeBoost();
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

        final int contextDoc = globalDoc-currentDocBase;
        if (collapseValues.advanceExact(contextDoc)) {
          final int collapseValue = (int) collapseValues.longValue();
          final long scoreDoc = cmap.get(collapseValue);
          dummy.score = Float.intBitsToFloat((int)(scoreDoc>>32));
          
        } else { // Null Group...
          
          if(mergeBoost.boost(globalDoc)) {
            //It's an elevated doc so no score is needed (and should not have been populated)
            dummy.score = 0F;
          } else if (nullPolicy == NullPolicy.COLLAPSE.getCode()) {
            dummy.score = nullScore;
          } else if(nullPolicy == NullPolicy.EXPAND.getCode()) {
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
   * Collapse on Ordinal value field.
   * @lucene.internal
   */
  static class OrdFieldValueCollector extends DelegatingCollector {
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

    private boolean collectElevatedDocsWhenCollapsing;

    private final BoostedDocsCollector boostedDocsCollector;

    public OrdFieldValueCollector(int maxDoc,
                                  int segments,
                                  DocValuesProducer collapseValuesProducer,
                                  int nullPolicy,
                                  GroupHeadSelector groupHeadSelector,
                                  SortSpec sortSpec,
                                  boolean needsScores4Collapsing,
                                  boolean needsScores,
                                  FieldType fieldType,
                                  IntIntHashMap boostDocsMap,
                                  FunctionQuery funcQuery, IndexSearcher searcher,
                                  boolean collectElevatedDocsWhenCollapsing) throws IOException{

      assert ! GroupHeadSelectorType.SCORE.equals(groupHeadSelector.type);

      this.collectElevatedDocsWhenCollapsing = collectElevatedDocsWhenCollapsing;
      this.maxDoc = maxDoc;
      this.contexts = new LeafReaderContext[segments];
      List<LeafReaderContext> con = searcher.getTopReaderContext().leaves();
      for(int i=0; i<con.size(); i++) {
        contexts[i] = con.get(i);
      }
      this.collapseValuesProducer = collapseValuesProducer;
      this.collapseValues = collapseValuesProducer.getSorted(null);
      if(collapseValues instanceof MultiDocValues.MultiSortedDocValues) {
        this.multiSortedDocValues = (MultiDocValues.MultiSortedDocValues)collapseValues;
        this.ordinalMap = multiSortedDocValues.mapping;
      }

      this.boostedDocsCollector = BoostedDocsCollector.build(boostDocsMap);
      
      int valueCount = collapseValues.getValueCount();
      this.nullPolicy = nullPolicy;
      this.needsScores4Collapsing = needsScores4Collapsing;
      this.needsScores = needsScores;
      if (null != sortSpec) {
        this.collapseStrategy = new OrdSortSpecStrategy(maxDoc, nullPolicy, valueCount, groupHeadSelector, this.needsScores4Collapsing, this.needsScores, boostedDocsCollector, sortSpec, searcher, collapseValues);
      } else if (funcQuery != null) {
        this.collapseStrategy =  new OrdValueSourceStrategy(maxDoc, nullPolicy, valueCount, groupHeadSelector, this.needsScores4Collapsing, this.needsScores, boostedDocsCollector, funcQuery, searcher, collapseValues);
      } else {
        NumberType numType = fieldType.getNumberType();
        if (null == numType) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "min/max must be either Int/Long/Float based field types");
        }
        switch (numType) {
          case INTEGER: {
            this.collapseStrategy = new OrdIntStrategy(maxDoc, nullPolicy, valueCount, groupHeadSelector, this.needsScores, boostedDocsCollector, collapseValues);
            break;
          }
          case FLOAT: {
            this.collapseStrategy = new OrdFloatStrategy(maxDoc, nullPolicy, valueCount, groupHeadSelector, this.needsScores, boostedDocsCollector, collapseValues);
            break;
          }
          case LONG: {
            this.collapseStrategy =  new OrdLongStrategy(maxDoc, nullPolicy, valueCount, groupHeadSelector, this.needsScores, boostedDocsCollector, collapseValues);
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

      if (collectElevatedDocsWhenCollapsing){
        // Check to see if we have documents boosted by the QueryElevationComponent (skip normal strategy based collection)
        if (-1 == ord) {
          if (boostedDocsCollector.collectInNullGroupIfBoosted(globalDoc)) return;
        } else {
          if (boostedDocsCollector.collectIfBoosted(ord, globalDoc)) return;
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
      IntFloatDynamicMap scores = collapseStrategy.getScores();
      FloatArrayList nullScores = collapseStrategy.getNullScores();
      float nullScore = collapseStrategy.getNullScore();
      final MergeBoost mergeBoost = boostedDocsCollector.getMergeBoost();

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
            dummy.score = scores.get(ord);
          } else if (mergeBoost.boost(globalDoc)) {
            //It's an elevated doc so no score is needed (and should not have been populated)
            dummy.score = 0F;
          } else if (nullPolicy == NullPolicy.COLLAPSE.getCode()) {
            dummy.score = nullScore;
          } else if(nullPolicy == NullPolicy.EXPAND.getCode()) {
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
   * @lucene.internal
   */
  static class IntFieldValueCollector extends DelegatingCollector {
    private LeafReaderContext[] contexts;
    private NumericDocValues collapseValues;
    private int maxDoc;
    private int nullPolicy;

    private IntFieldValueStrategy collapseStrategy;
    private boolean needsScores4Collapsing;
    private boolean needsScores;
    private String collapseField;
    
    private final BoostedDocsCollector boostedDocsCollector;
    private boolean collectElevatedDocsWhenCollapsing;

    public IntFieldValueCollector(int maxDoc,
                                  int size,
                                  int segments,
                                  int nullPolicy,
                                  String collapseField,
                                  GroupHeadSelector groupHeadSelector,
                                  SortSpec sortSpec,
                                  boolean needsScores4Collapsing,
                                  boolean needsScores,
                                  FieldType fieldType,
                                  IntIntHashMap boostDocsMap,
                                  FunctionQuery funcQuery,
                                  IndexSearcher searcher,
                                  boolean collectElevatedDocsWhenCollapsing) throws IOException{
      this.collectElevatedDocsWhenCollapsing = collectElevatedDocsWhenCollapsing;

      assert ! GroupHeadSelectorType.SCORE.equals(groupHeadSelector.type);

      this.maxDoc = maxDoc;
      this.contexts = new LeafReaderContext[segments];
      List<LeafReaderContext> con = searcher.getTopReaderContext().leaves();
      for(int i=0; i<con.size(); i++) {
        contexts[i] = con.get(i);
      }
      this.collapseField = collapseField;
      this.nullPolicy = nullPolicy;
      this.needsScores4Collapsing = needsScores4Collapsing;
      this.needsScores = needsScores;

      this.boostedDocsCollector = BoostedDocsCollector.build(boostDocsMap);
      
      if (null != sortSpec) {
        this.collapseStrategy = new IntSortSpecStrategy(maxDoc, size, collapseField, nullPolicy, groupHeadSelector, this.needsScores4Collapsing, this.needsScores, boostedDocsCollector, sortSpec, searcher);
      } else if (funcQuery != null) {
        this.collapseStrategy =  new IntValueSourceStrategy(maxDoc, size, collapseField, nullPolicy, groupHeadSelector, this.needsScores4Collapsing, this.needsScores, boostedDocsCollector, funcQuery, searcher);
      } else {
        NumberType numType = fieldType.getNumberType();
        assert null != numType; // shouldn't make it here for non-numeric types
        switch (numType) {
          case INTEGER: {
            this.collapseStrategy = new IntIntStrategy(maxDoc, size, collapseField, nullPolicy, groupHeadSelector, this.needsScores, boostedDocsCollector);
            break;
          }
          case FLOAT: {
            this.collapseStrategy = new IntFloatStrategy(maxDoc, size, collapseField, nullPolicy, groupHeadSelector, this.needsScores, boostedDocsCollector);
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
      final int globalDoc = contextDoc+this.docBase;
      if (collapseValues.advanceExact(contextDoc)) {
        final int collapseKey = (int) collapseValues.longValue();
        // Check to see if we have documents boosted by the QueryElevationComponent (skip normal strategy based collection)
        if (boostedDocsCollector.collectIfBoosted(collapseKey, globalDoc)) return;
        collapseStrategy.collapse(collapseKey, contextDoc, globalDoc);
        
      } else { // Null Group...

        if (collectElevatedDocsWhenCollapsing){
          // Check to see if we have documents boosted by the QueryElevationComponent (skip normal strategy based collection)
          if (boostedDocsCollector.collectInNullGroupIfBoosted(globalDoc)) return;
        }
        if (NullPolicy.IGNORE.getCode() != nullPolicy) {
          collapseStrategy.collapseNullGroup(contextDoc, globalDoc);
        }
      }

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
      IntFloatDynamicMap scores = collapseStrategy.getScores();
      FloatArrayList nullScores = collapseStrategy.getNullScores();
      float nullScore = collapseStrategy.getNullScore();
      final MergeBoost mergeBoost = boostedDocsCollector.getMergeBoost();

      while((globalDoc = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {

        while(globalDoc >= nextDocBase) {
          currentContext++;
          currentDocBase = contexts[currentContext].docBase;
          nextDocBase = currentContext+1 < contexts.length ? contexts[currentContext+1].docBase : maxDoc;
          leafDelegate = delegate.getLeafCollector(contexts[currentContext]);
          leafDelegate.setScorer(dummy);
          this.collapseValues = DocValues.getNumeric(contexts[currentContext].reader(), this.collapseField);
        }

        final int contextDoc = globalDoc-currentDocBase;

        if(this.needsScores){
          if (collapseValues.advanceExact(contextDoc)) {
            final int collapseValue = (int) collapseValues.longValue();
            
            final int pointer = cmap.get(collapseValue);
            dummy.score = scores.get(pointer);

          } else { // Null Group...
            
            if (mergeBoost.boost(globalDoc)) {
              //It's an elevated doc so no score is needed (and should not have been populated)
              dummy.score = 0F;
            } else if (nullPolicy == NullPolicy.COLLAPSE.getCode()) {
              dummy.score = nullScore;
            } else if(nullPolicy == NullPolicy.EXPAND.getCode()) {
              dummy.score = nullScores.get(nullScoreIndex++);
            }
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
   * Base class for collectors that will do collapsing using "block indexed" documents
   *
   * @lucene.internal
   */
  private static abstract class AbstractBlockCollector extends DelegatingCollector {
    
    protected final BlockGroupState currentGroupState = new BlockGroupState();
    protected final String collapseField;
    protected final boolean needsScores;
    protected final boolean expandNulls;
    private final MergeBoost boostDocs;
    private int docBase = 0;

    protected AbstractBlockCollector(final String collapseField,
                                     final int nullPolicy,
                                     final IntIntHashMap boostDocsMap,
                                     final boolean needsScores) {

      
      this.collapseField = collapseField;
      this.needsScores = needsScores;

      assert nullPolicy != NullPolicy.COLLAPSE.getCode();
      assert nullPolicy == NullPolicy.IGNORE.getCode() || nullPolicy == NullPolicy.EXPAND.getCode();
      this.expandNulls = (NullPolicy.EXPAND.getCode() == nullPolicy);
      this.boostDocs = BoostedDocsCollector.build(boostDocsMap).getMergeBoost();
      
      currentGroupState.resetForNewGroup();
    }
    
    @Override public ScoreMode scoreMode() { return needsScores ? ScoreMode.COMPLETE : super.scoreMode(); }

    /**
     * If we have a candidate match, delegate the collection of that match.
     */
    protected void maybeDelegateCollect() throws IOException {
      if (currentGroupState.isCurrentDocCollectable()) {
        delegateCollect();
      }
    }
    /**
     * Immediately delegate the collection of the current doc
     */
    protected void delegateCollect() throws IOException {
      // ensure we have the 'correct' scorer
      // (our supper class may have set the "real" scorer on our leafDelegate
      // and it may have an incorrect docID)
      leafDelegate.setScorer(currentGroupState);
      leafDelegate.collect(currentGroupState.docID());
    }

    /** 
     * NOTE: collects the best doc for the last group in the previous segment
     * subclasses must call super <em>BEFORE</em> they make any changes to their own state that might influence
     * collection
     */
    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      maybeDelegateCollect();
      // Now setup for the next segment.
      currentGroupState.resetForNewGroup();
      this.docBase = context.docBase;
      super.doSetNextReader(context);
    }

    /** 
     * Acts as an id iterator over the boosted docs
     *
     * @param contextDoc the context specific docId to check for, iterator is advanced to this id
     * @return true if the contextDoc is boosted, false otherwise.
     */
    protected boolean isBoostedAdvanceExact(final int contextDoc) {
      return boostDocs.boost(contextDoc + docBase);
    }

    @Override
    public void finish() throws IOException {
      // Deal with last group (if any)...
      maybeDelegateCollect();
      
      super.finish();
    }
    
    /**
     * Encapsulates basic state information about the current group, and the "best matching" document in that group (so far)
     */
    protected static final class BlockGroupState extends ScoreAndDoc {
      /** 
       * Specific values have no intrinsic meaning, but can <em>only</em> 
       * be considered if the current docID in {@link #docID} is non-negative
       */
      private int currentGroup = 0;
      private boolean groupHasBoostedDocs;
      public void setCurrentGroup(final int groupId) {
        this.currentGroup = groupId;
      }
      public int getCurrentGroup() {
        assert -1 < docID();
        return this.currentGroup;
      }
      public void setBestDocForCurrentGroup(final int contextDoc, final boolean isBoosted) {
        this.docId = contextDoc;
        this.groupHasBoostedDocs |= isBoosted;
      }
      
      public void resetForNewGroup() {
        this.docId = -1;
        this.score = Float.MIN_VALUE;
        this.groupHasBoostedDocs = false;
      }
      
      public boolean hasBoostedDocs() {
        assert -1 < docID();
        return groupHasBoostedDocs;
      }
      
      /** 
       * Returns true if we have a valid ("best match") docId for the current group and there are no boosted docs 
       * for this group (If the current doc was boosted, it should have already been collected)
       */
      public boolean isCurrentDocCollectable() {
        return (-1 < docID() && ! groupHasBoostedDocs);
      }
    }
  }
  
  /**
   * Collapses groups on a block using a field that has values unique to that block (example: <code>_root_</code>)
   * choosing the group head based on score
   *
   * @lucene.internal
   */
  static abstract class AbstractBlockScoreCollector extends AbstractBlockCollector {

    public AbstractBlockScoreCollector(final String collapseField, final int nullPolicy, final IntIntHashMap boostDocsMap) {
      super(collapseField, nullPolicy, boostDocsMap, true);
    }
    
    private void setCurrentGroupBestMatch(final int contextDocId, final float score, final boolean isBoosted) {
      currentGroupState.setBestDocForCurrentGroup(contextDocId, isBoosted);
      currentGroupState.score = score;
    }

    /**
     * This method should be called by subclasses for each doc + group encountered
     * @param contextDoc a valid doc id relative to the current reader context
     * @param docGroup some uique identifier for the group - the base class makes no assumptions about it's meaning
     * @see #collectDocWithNullGroup
     */
    protected void collectDocWithGroup(int contextDoc, int docGroup) throws IOException {
      assert 0 <= contextDoc;
      
      final boolean isBoosted = isBoostedAdvanceExact(contextDoc);

      if (-1 < currentGroupState.docID() && docGroup == currentGroupState.getCurrentGroup()) {
        // we have an existing group, and contextDoc is in that group.

        if (isBoosted) {
          // this doc is the best and should be immediately collected regardless of score
          setCurrentGroupBestMatch(contextDoc, scorer.score(), isBoosted);
          delegateCollect();

        } else if (currentGroupState.hasBoostedDocs()) {
          // No-Op: nothing about this doc matters since we've already collected boosted docs in this group

          // No-Op
        } else {
          // check if this doc the new 'best' doc in this group...
          final float score = scorer.score();
          if (score > currentGroupState.score) {
            setCurrentGroupBestMatch(contextDoc, scorer.score(), isBoosted);
          }
        }
        
      } else {
        // We have a document that starts a new group (or may be the first doc+group we've collected this segment)
        
        // first collect the prior group if needed...
        maybeDelegateCollect();
        
        // then setup the new group and current best match
        currentGroupState.resetForNewGroup();
        currentGroupState.setCurrentGroup(docGroup);
        setCurrentGroupBestMatch(contextDoc, scorer.score(), isBoosted);
        
        if (isBoosted) { // collect immediately
          delegateCollect();
        }
      }
    }

    /**
     * This method should be called by subclasses for each doc encountered that is not in a group (ie: null group)
     * @param contextDoc a valid doc id relative to the current reader context
     * @see #collectDocWithGroup
     */
    protected void collectDocWithNullGroup(int contextDoc) throws IOException {
      assert 0 <= contextDoc;

      // NOTE: with 'null group' docs, it doesn't matter if they are boosted since we don't suppor collapsing nulls
      
      // this doc is definitely not part of any prior group, so collect if needed...
      maybeDelegateCollect();

      if (expandNulls) {
        // set & immediately collect our current doc...
        setCurrentGroupBestMatch(contextDoc, scorer.score(), false);
        delegateCollect();
        
      } else {
        // we're ignoring nulls, so: No-Op.
      }

      // either way re-set for the next doc / group
      currentGroupState.resetForNewGroup();
    }
   
  }

  /** 
   * A block based score collector that uses a field's "ord" as the group ids
   * @lucene.internal
   */
  static class BlockOrdScoreCollector extends AbstractBlockScoreCollector {
    private SortedDocValues segmentValues;
    
    public BlockOrdScoreCollector(final String collapseField, final int nullPolicy, final IntIntHashMap boostDocsMap) throws IOException {
      super(collapseField, nullPolicy, boostDocsMap);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.segmentValues = DocValues.getSorted(context.reader(), collapseField);
    }
    
    @Override
    public void collect(int contextDoc) throws IOException {
      if (segmentValues.advanceExact(contextDoc)) {
        int ord = segmentValues.ordValue();
        collectDocWithGroup(contextDoc, ord);
      } else {
        collectDocWithNullGroup(contextDoc);
      }
    }
  }
  /** 
   * A block based score collector that uses a field's numeric value as the group ids 
   * @lucene.internal
   */
  static class BlockIntScoreCollector extends AbstractBlockScoreCollector {
    private NumericDocValues segmentValues;
    
    public BlockIntScoreCollector(final String collapseField, final int nullPolicy, final IntIntHashMap boostDocsMap) throws IOException {
      super(collapseField, nullPolicy, boostDocsMap);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.segmentValues = DocValues.getNumeric(context.reader(), collapseField);
    }
    
    @Override
    public void collect(int contextDoc) throws IOException {
      if (segmentValues.advanceExact(contextDoc)) {
        int group = (int) segmentValues.longValue();
        collectDocWithGroup(contextDoc, group);
      } else {
        collectDocWithNullGroup(contextDoc);
      }
    }
  }

  /**
   * <p>
   * Collapses groups on a block using a field that has values unique to that block (example: <code>_root_</code>)
   * choosing the group head based on a {@link SortSpec} 
   * (which can be synthetically created for min/max group head selectors using {@link #getSort})
   * </p>
   * <p>
   * Note that since this collector does a single pass, and unlike other collectors doesn't need to maintain a large data 
   * structure of scores (for all matching docs) when they might be needed for the response, it has no need to distinguish 
   * between the concepts of <code>needsScores4Collapsing</code> vs </code>needsScores</code>
   * </p>
   * @lucene.internal
   */
  static abstract class AbstractBlockSortSpecCollector extends AbstractBlockCollector {

    /**
     * Helper method for extracting a {@link Sort} out of a {@link SortSpec} <em>or</em> creating one synthetically for
     * "min/max" {@link GroupHeadSelector} against a {@link FunctionQuery} <em>or</em> simple field name.
     *
     * @return appropriate (already re-written) Sort to use with a AbstractBlockSortSpecCollector
     */
    public static Sort getSort(final GroupHeadSelector groupHeadSelector,
                               final SortSpec sortSpec,
                               final FunctionQuery funcQuery,
                               final SolrIndexSearcher searcher) throws IOException {
      if (null != sortSpec) {
        assert GroupHeadSelectorType.SORT.equals(groupHeadSelector.type);

        // a "feature" of SortSpec is that getSort() is null if we're just using 'score desc'
        if (null == sortSpec.getSort()) {
          return Sort.RELEVANCE.rewrite(searcher);
        }
        return sortSpec.getSort().rewrite(searcher);
        
      } // else: min/max on field or value source...

      assert GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type);
      assert ! CollapseScore.wantsCScore(groupHeadSelector.selectorText);
        
      final boolean reverse = GroupHeadSelectorType.MAX.equals(groupHeadSelector.type);
      final SortField sf = (null != funcQuery)
        ? funcQuery.getValueSource().getSortField(reverse)
        : searcher.getSchema().getField(groupHeadSelector.selectorText).getSortField(reverse);
      
      return (new Sort(sf)).rewrite(searcher);
    }

    private final BlockBasedSortFieldsCompare sortsCompare;

    public AbstractBlockSortSpecCollector(final String collapseField,
                                          final int nullPolicy,
                                          final IntIntHashMap boostDocsMap,
                                          final Sort sort,
                                          final boolean needsScores) {
      super(collapseField, nullPolicy, boostDocsMap, needsScores);
      this.sortsCompare = new BlockBasedSortFieldsCompare(sort.getSort());
      
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      sortsCompare.setScorer(scorer);
      super.setScorer(scorer);
    }
    
    private void setCurrentGroupBestMatch(final int contextDocId, final boolean isBoosted) throws IOException {
      currentGroupState.setBestDocForCurrentGroup(contextDocId, isBoosted);
      if (needsScores) {
        currentGroupState.score = scorer.score();
      }
    }
    
    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.sortsCompare.setNextReader(context);
    }

    /**
     * This method should be called by subclasses for each doc + group encountered
     * @param contextDoc a valid doc id relative to the current reader context
     * @param docGroup some uique identifier for the group - the base class makes no assumptions about it's meaning
     * @see #collectDocWithNullGroup
     */
    protected void collectDocWithGroup(int contextDoc, int docGroup) throws IOException {
      assert 0 <= contextDoc;
      
      final boolean isBoosted = isBoostedAdvanceExact(contextDoc);
      
      if (-1 < currentGroupState.docID() && docGroup == currentGroupState.getCurrentGroup()) {
        // we have an existing group, and contextDoc is in that group.

        if (isBoosted) {
          // this doc is the best and should be immediately collected regardless of sort values
          setCurrentGroupBestMatch(contextDoc, isBoosted);
          delegateCollect();

        } else if (currentGroupState.hasBoostedDocs()) {
          // No-Op: nothing about this doc matters since we've already collected boosted docs in this group

          // No-Op
        } else {
          // check if it's the new 'best' doc in this group...
          if (sortsCompare.testAndSetGroupValues(contextDoc)) {
            setCurrentGroupBestMatch(contextDoc, isBoosted);
          }
        }
        
      } else {
        // We have a document that starts a new group (or may be the first doc+group we've collected this segmen)
        
        // first collect the prior group if needed...
        maybeDelegateCollect();
        
        // then setup the new group and current best match
        currentGroupState.resetForNewGroup();
        currentGroupState.setCurrentGroup(docGroup);
        sortsCompare.setGroupValues(contextDoc);
        setCurrentGroupBestMatch(contextDoc, isBoosted);

        if (isBoosted) { // collect immediately
          delegateCollect();
        }
      }
    }

    /**
     * This method should be called by subclasses for each doc encountered that is not in a group (ie: null group)
     * @param contextDoc a valid doc id relative to the current reader context
     * @see #collectDocWithGroup
     */
    protected void collectDocWithNullGroup(int contextDoc) throws IOException {
      assert 0 <= contextDoc;
      
      // NOTE: with 'null group' docs, it doesn't matter if they are boosted since we don't suppor collapsing nulls
      
      // this doc is definitely not part of any prior group, so collect if needed...
      maybeDelegateCollect();

      if (expandNulls) {
        // set & immediately collect our current doc...
        setCurrentGroupBestMatch(contextDoc, false);
        // NOTE: sort values don't matter
        delegateCollect();
        
      } else {
        // we're ignoring nulls, so: No-Op.
      }

      // either way re-set for the next doc / group
      currentGroupState.resetForNewGroup();
    }
   
  }
  
  /** 
   * A block based score collector that uses a field's "ord" as the group ids
   * @lucene.internal
   */
  static class BlockOrdSortSpecCollector extends AbstractBlockSortSpecCollector {
    private SortedDocValues segmentValues;
    
    public BlockOrdSortSpecCollector(final String collapseField,
                                     final int nullPolicy,
                                     final IntIntHashMap boostDocsMap,
                                     final Sort sort,
                                     final boolean needsScores) throws IOException {
      super(collapseField, nullPolicy, boostDocsMap, sort, needsScores);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.segmentValues = DocValues.getSorted(context.reader(), collapseField);
    }
    
    @Override
    public void collect(int contextDoc) throws IOException {
      if (segmentValues.advanceExact(contextDoc)) {
        int ord = segmentValues.ordValue();
        collectDocWithGroup(contextDoc, ord);
      } else {
        collectDocWithNullGroup(contextDoc);
      }
    }
  }
  /** 
   * A block based score collector that uses a field's numeric value as the group ids 
   * @lucene.internal
   */
  static class BlockIntSortSpecCollector extends AbstractBlockSortSpecCollector {
    private NumericDocValues segmentValues;
    
    public BlockIntSortSpecCollector(final String collapseField,
                                     final int nullPolicy,
                                     final IntIntHashMap boostDocsMap,
                                     final Sort sort,
                                     final boolean needsScores) throws IOException {
      super(collapseField, nullPolicy, boostDocsMap, sort, needsScores);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      this.segmentValues = DocValues.getNumeric(context.reader(), collapseField);
    }
    
    @Override
    public void collect(int contextDoc) throws IOException {
      if (segmentValues.advanceExact(contextDoc)) {
        int group = (int) segmentValues.longValue();
        collectDocWithGroup(contextDoc, group);
      } else {
        collectDocWithNullGroup(contextDoc);
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

      // block collapsing logic is much simpler and uses less memory, but is only viable in specific situations
      final boolean blockCollapse = (("_root_".equals(collapseField) || HINT_BLOCK.equals(hint))
                                     // because we currently handle all min/max cases using
                                     // AbstractBlockSortSpecCollector, we can't handle functions wrapping cscore()
                                     // (for the same reason cscore() isn't supported in 'sort' local param)
                                     && ( ! CollapseScore.wantsCScore(groupHeadSelector.selectorText) )
                                     //
                                     && NullPolicy.COLLAPSE.getCode() != nullPolicy);
      if (HINT_BLOCK.equals(hint) && ! blockCollapse) {
        log.debug("Query specifies hint={} but other local params prevent the use block based collapse", HINT_BLOCK);
      }
      
      FieldType collapseFieldType = searcher.getSchema().getField(collapseField).getType();

      if(collapseFieldType instanceof StrField) {
        // if we are using blockCollapse, then there is no need to bother with TOP_FC
        if(HINT_TOP_FC.equals(hint) && ! blockCollapse) {
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

      SolrRequestInfo req = SolrRequestInfo.getRequestInfo();
      boolean collectElevatedDocsWhenCollapsing = req != null && req.getReq().getParams().getBool(COLLECT_ELEVATED_DOCS_WHEN_COLLAPSING, true);

      if (GroupHeadSelectorType.SCORE.equals(groupHeadSelector.type)) {

        if (collapseFieldType instanceof StrField) {
          if (blockCollapse) {
            return new BlockOrdScoreCollector(collapseField, nullPolicy, boostDocs);
          }
          return new OrdScoreCollector(maxDoc, leafCount, docValuesProducer, nullPolicy, boostDocs, searcher, collectElevatedDocsWhenCollapsing);

        } else if (isNumericCollapsible(collapseFieldType)) {
          if (blockCollapse) {
            return new BlockIntScoreCollector(collapseField, nullPolicy, boostDocs);
          }

          return new IntScoreCollector(maxDoc, leafCount, nullPolicy, size, collapseField, boostDocs, searcher, collectElevatedDocsWhenCollapsing);

        } else {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Collapsing field should be of either String, Int or Float type");
        }

      } else { // min, max, sort, etc.. something other then just "score"

        if (collapseFieldType instanceof StrField) {
          if (blockCollapse) {
            // NOTE: for now we don't worry about wether this is a sortSpec of min/max groupHeadSelector,
            // we use a "sort spec' based block collector unless/until there is some (performance?) reason to specialize
            return new BlockOrdSortSpecCollector(collapseField, nullPolicy, boostDocs,
                                                 BlockOrdSortSpecCollector.getSort(groupHeadSelector,
                                                                                   sortSpec, funcQuery, searcher),
                                                 needsScores || needsScores4Collapsing);
          }

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
                                            searcher,
                                            collectElevatedDocsWhenCollapsing);

        } else if (isNumericCollapsible(collapseFieldType)) {

          if (blockCollapse) {
            // NOTE: for now we don't worry about wether this is a sortSpec of min/max groupHeadSelector,
            // we use a "sort spec' based block collector unless/until there is some (performance?) reason to specialize
            return new BlockIntSortSpecCollector(collapseField, nullPolicy, boostDocs,
                                                 BlockOrdSortSpecCollector.getSort(groupHeadSelector,
                                                                                   sortSpec, funcQuery, searcher),
                                                 needsScores || needsScores4Collapsing);
          }

          return new IntFieldValueCollector(maxDoc,
                                            size,
                                            leafCount,
                                            nullPolicy,
                                            collapseField,
                                            groupHeadSelector,
                                            sortSpec,
                                            needsScores4Collapsing,
                                            needsScores,
                                            minMaxFieldType,
                                            boostDocs,
                                            funcQuery,
                                            searcher,
                                            collectElevatedDocsWhenCollapsing);
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
    @SuppressWarnings({"unchecked"})
    public boolean setupIfNeeded(final GroupHeadSelector groupHeadSelector,
                                 @SuppressWarnings({"rawtypes"})final Map readerContext) {
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
    protected IntIntDynamicMap ords;
    protected Scorable scorer;
    protected FloatArrayList nullScores;
    protected float nullScore;
    protected IntFloatDynamicMap scores;
    protected FixedBitSet collapsedSet;
    protected int nullDoc = -1;
    protected boolean needsScores;
    
    private final BoostedDocsCollector boostedDocsCollector;

    public abstract void collapse(int ord, int contextDoc, int globalDoc) throws IOException;
    public abstract void setNextReader(LeafReaderContext context) throws IOException;

    public OrdFieldValueStrategy(int maxDoc,
                                 int valueCount,
                                 int nullPolicy,
                                 boolean needsScores,
                                 BoostedDocsCollector boostedDocsCollector,
                                 SortedDocValues values) {
      this.ords = new IntIntDynamicMap(valueCount, -1);
      this.nullPolicy = nullPolicy;
      this.needsScores = needsScores;
      this.collapsedSet = new FixedBitSet(maxDoc);

      this.boostedDocsCollector = boostedDocsCollector;
      
      if (this.needsScores) {
        this.scores = new IntFloatDynamicMap(valueCount, 0.0f);
        if(nullPolicy == NullPolicy.EXPAND.getCode()) {
          nullScores = new FloatArrayList();
        }
      }
    }

    public FixedBitSet getCollapsedSet() {
      // Handle the boosted docs.
      boostedDocsCollector.purgeGroupsThatHaveBoostedDocs(collapsedSet,
                                                          (ord) -> { ords.remove(ord); },
                                                          () -> { nullDoc = -1; });
      
      //Build the sorted DocSet of group heads.
      if(nullDoc > -1) {
        this.collapsedSet.set(nullDoc);
      }
      ords.forEachValue(doc -> collapsedSet.set(doc));

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

    public IntFloatDynamicMap getScores() {
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
    private IntIntDynamicMap ordVals;

    public OrdIntStrategy(int maxDoc,
                          int nullPolicy,
                          int valueCount,
                          GroupHeadSelector groupHeadSelector,
                          boolean needsScores,
                          BoostedDocsCollector boostedDocsCollector,
                          SortedDocValues values) throws IOException {
      super(maxDoc, valueCount, nullPolicy, needsScores, boostedDocsCollector, values);
      this.field = groupHeadSelector.selectorText;

      assert GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type);

      if (GroupHeadSelectorType.MAX.equals(groupHeadSelector.type)) {
        comp = new MaxIntComp();
        this.ordVals = new IntIntDynamicMap(valueCount, Integer.MIN_VALUE);
      } else {
        comp = new MinIntComp();
        this.ordVals = new IntIntDynamicMap(valueCount, Integer.MAX_VALUE);
        this.nullVal = Integer.MAX_VALUE;
      }
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.minMaxValues = DocValues.getNumeric(context.reader(), this.field);
    }

    public void collapse(int ord, int contextDoc, int globalDoc) throws IOException {

      int currentVal;
      if (minMaxValues.advanceExact(contextDoc)) {
        currentVal = (int) minMaxValues.longValue();
      } else {
        currentVal = 0;
      }

      if(ord > -1) {
        if(comp.test(currentVal, ordVals.get(ord))) {
          ords.put(ord, globalDoc);
          ordVals.put(ord, currentVal);
          if(needsScores) {
            scores.put(ord, scorer.score());
          }
        }
      } else if(this.nullPolicy == NullPolicy.COLLAPSE.getCode()) {
        if(comp.test(currentVal, nullVal)) {
          nullVal = currentVal;
          nullDoc = globalDoc;
          if(needsScores) {
            nullScore = scorer.score();
          }
        }
      } else if(this.nullPolicy == NullPolicy.EXPAND.getCode()) {
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
    private IntFloatDynamicMap ordVals;

    public OrdFloatStrategy(int maxDoc,
                            int nullPolicy,
                            int valueCount,
                            GroupHeadSelector groupHeadSelector,
                            boolean needsScores,
                            BoostedDocsCollector boostedDocsCollector,
                            SortedDocValues values) throws IOException {
      super(maxDoc, valueCount, nullPolicy, needsScores, boostedDocsCollector, values);
      this.field = groupHeadSelector.selectorText;

      assert GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type);

      if (GroupHeadSelectorType.MAX.equals(groupHeadSelector.type)) {
        comp = new MaxFloatComp();
        this.ordVals = new IntFloatDynamicMap(valueCount, -Float.MAX_VALUE);
        this.nullVal = -Float.MAX_VALUE;
      } else {
        comp = new MinFloatComp();
        this.ordVals = new IntFloatDynamicMap(valueCount, Float.MAX_VALUE);
        this.nullVal = Float.MAX_VALUE;
      }
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.minMaxValues = DocValues.getNumeric(context.reader(), this.field);
    }

    public void collapse(int ord, int contextDoc, int globalDoc) throws IOException {

      int currentMinMax;
      if (minMaxValues.advanceExact(contextDoc)) {
        currentMinMax = (int) minMaxValues.longValue();
      } else {
        currentMinMax = 0;
      }

      float currentVal = Float.intBitsToFloat(currentMinMax);

      if(ord > -1) {
        if(comp.test(currentVal, ordVals.get(ord))) {
          ords.put(ord, globalDoc);
          ordVals.put(ord, currentVal);
          if(needsScores) {
            scores.put(ord, scorer.score());
          }
        }
      } else if(this.nullPolicy == NullPolicy.COLLAPSE.getCode()) {
        if(comp.test(currentVal, nullVal)) {
          nullVal = currentVal;
          nullDoc = globalDoc;
          if(needsScores) {
            nullScore = scorer.score();
          }
        }
      } else if(this.nullPolicy == NullPolicy.EXPAND.getCode()) {
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
    private IntLongDynamicMap ordVals;

    public OrdLongStrategy(int maxDoc,
                           int nullPolicy,
                           int valueCount,
                           GroupHeadSelector groupHeadSelector,
                           boolean needsScores,
                           BoostedDocsCollector boostedDocsCollector,
                           SortedDocValues values) throws IOException {
      super(maxDoc, valueCount, nullPolicy, needsScores, boostedDocsCollector, values);
      this.field = groupHeadSelector.selectorText;

      assert GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type);

      if (GroupHeadSelectorType.MAX.equals(groupHeadSelector.type)) {
        comp = new MaxLongComp();
        this.ordVals = new IntLongDynamicMap(valueCount, Long.MIN_VALUE);
      } else {
        this.nullVal = Long.MAX_VALUE;
        comp = new MinLongComp();
        this.ordVals = new IntLongDynamicMap(valueCount, Long.MAX_VALUE);
      }
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.minMaxVals = DocValues.getNumeric(context.reader(), this.field);
    }

    public void collapse(int ord, int contextDoc, int globalDoc) throws IOException {

      long currentVal;
      if (minMaxVals.advanceExact(contextDoc)) {
        currentVal = minMaxVals.longValue();
      } else {
        currentVal = 0;
      }

      if(ord > -1) {
        if(comp.test(currentVal, ordVals.get(ord))) {
          ords.put(ord, globalDoc);
          ordVals.put(ord, currentVal);
          if(needsScores) {
            scores.put(ord, scorer.score());
          }
        }
      } else if(this.nullPolicy == NullPolicy.COLLAPSE.getCode()) {
        if(comp.test(currentVal, nullVal)) {
          nullVal = currentVal;
          nullDoc = globalDoc;
          if(needsScores) {
            nullScore = scorer.score();
          }
        }
      } else if(this.nullPolicy == NullPolicy.EXPAND.getCode()) {
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
    private IntFloatDynamicMap ordVals;
    @SuppressWarnings({"rawtypes"})
    private Map rcontext;
    private final CollapseScore collapseScore = new CollapseScore();
    private boolean needsScores4Collapsing;

    public OrdValueSourceStrategy(int maxDoc,
                                  int nullPolicy,
                                  int valueCount,
                                  GroupHeadSelector groupHeadSelector,
                                  boolean needsScores4Collapsing,
                                  boolean needsScores,
                                  BoostedDocsCollector boostedDocsCollector,
                                  FunctionQuery funcQuery,
                                  IndexSearcher searcher,
                                  SortedDocValues values) throws IOException {
      super(maxDoc, valueCount, nullPolicy, needsScores, boostedDocsCollector, values);
      this.needsScores4Collapsing = needsScores4Collapsing;
      this.valueSource = funcQuery.getValueSource();
      this.rcontext = ValueSource.newContext(searcher);

      assert GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type);

      if (GroupHeadSelectorType.MAX.equals(groupHeadSelector.type)) {
        comp = new MaxFloatComp();
        this.ordVals = new IntFloatDynamicMap(valueCount, -Float.MAX_VALUE);
      } else {
        this.nullVal = Float.MAX_VALUE;
        comp = new MinFloatComp();
        this.ordVals = new IntFloatDynamicMap(valueCount, Float.MAX_VALUE);
      }

      collapseScore.setupIfNeeded(groupHeadSelector, rcontext);
    }

    @SuppressWarnings({"unchecked"})
    public void setNextReader(LeafReaderContext context) throws IOException {
      functionValues = this.valueSource.getValues(rcontext, context);
    }

    public void collapse(int ord, int contextDoc, int globalDoc) throws IOException {
      
      float score = 0;

      if (needsScores4Collapsing) {
        score = scorer.score();
        this.collapseScore.score = score;
      }

      float currentVal = functionValues.floatVal(contextDoc);

      if(ord > -1) {
        if(comp.test(currentVal, ordVals.get(ord))) {
          ords.put(ord, globalDoc);
          ordVals.put(ord, currentVal);
          if(needsScores) {
            if (!needsScores4Collapsing) {
              score = scorer.score();
            }
            scores.put(ord, score);
          }
        }
      } else if(this.nullPolicy == NullPolicy.COLLAPSE.getCode()) {
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
      } else if(this.nullPolicy == NullPolicy.EXPAND.getCode()) {
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
                               int valueCount,
                               GroupHeadSelector groupHeadSelector,
                               boolean needsScores4Collapsing,
                               boolean needsScores,
                               BoostedDocsCollector boostedDocsCollector,
                               SortSpec sortSpec,
                               IndexSearcher searcher,
                               SortedDocValues values) throws IOException {
      super(maxDoc, valueCount, nullPolicy, needsScores, boostedDocsCollector, values);
      this.needsScores4Collapsing = needsScores4Collapsing;

      assert GroupHeadSelectorType.SORT.equals(groupHeadSelector.type);

      this.sort = rewriteSort(sortSpec, searcher);

      this.compareState = new SortFieldsCompare(sort.getSort(), valueCount);
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

      if (needsScores4Collapsing) {
        this.score = scorer.score();
      }

      if (ord > -1) { // real collapseKey
        if (-1 == ords.get(ord)) {
          // we've never seen this ord (aka: collapseKey) before, treat it as group head for now
          compareState.setGroupValues(ord, contextDoc);
          ords.put(ord, globalDoc);
          if (needsScores) {
            if (!needsScores4Collapsing) {
              this.score = scorer.score();
            }
            scores.put(ord, score);
          }
        } else {
          // test this ord to see if it's a new group leader
          if (compareState.testAndSetGroupValues(ord, contextDoc)) {//TODO X
            ords.put(ord, globalDoc);
            if (needsScores) {
              if (!needsScores4Collapsing) {
                this.score = scorer.score();
              }
              scores.put(ord, score);
            }
          }
        }
      } else if (this.nullPolicy == NullPolicy.COLLAPSE.getCode()) {
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
      } else if(this.nullPolicy == NullPolicy.EXPAND.getCode()) {
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
    protected IntFloatDynamicMap scores;
    protected FixedBitSet collapsedSet;
    protected int nullDoc = -1;
    protected boolean needsScores;
    protected String collapseField;
    protected IntIntDynamicMap docs;
    
    private final BoostedDocsCollector boostedDocsCollector;

    public abstract void collapseNullGroup(int contextDoc, int globalDoc) throws IOException;
    public abstract void collapse(int collapseKey, int contextDoc, int globalDoc) throws IOException;
    public abstract void setNextReader(LeafReaderContext context) throws IOException;

    public IntFieldValueStrategy(int maxDoc,
                                 int size,
                                 String collapseField,
                                 int nullPolicy,
                                 boolean needsScores,
                                 BoostedDocsCollector boostedDocsCollector) {
      this.collapseField = collapseField;
      this.nullPolicy = nullPolicy;
      this.needsScores = needsScores;
      this.collapsedSet = new FixedBitSet(maxDoc);
      this.cmap = new IntIntHashMap(size);
      this.docs = new IntIntDynamicMap(size, 0);

      this.boostedDocsCollector = boostedDocsCollector;

      if(needsScores) {
        this.scores = new IntFloatDynamicMap(size, 0.0f);
        if(nullPolicy == NullPolicy.EXPAND.getCode()) {
          nullScores = new FloatArrayList();
        }
      }
    }

    public FixedBitSet getCollapsedSet() {

      // Handle the boosted docs.
      boostedDocsCollector.purgeGroupsThatHaveBoostedDocs(collapsedSet,
                                                          (key) -> { cmap.remove(key); },
                                                          () -> { nullDoc = -1; });
      
      //Build the sorted DocSet of group heads.
      if(nullDoc > -1) {
        this.collapsedSet.set(nullDoc);
      }
      Iterator<IntIntCursor> it1 = cmap.iterator();
      while(it1.hasNext()) {
        IntIntCursor cursor = it1.next();
        int pointer = cursor.value;
        collapsedSet.set(docs.get(pointer));
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

    public IntFloatDynamicMap getScores() {
      return scores;
    }

    public IntIntDynamicMap getDocs() { return docs;}

  }

  /*
   *  Strategy for collapsing on a 32 bit numeric field and selecting the group head based
   *  on the min/max value of a 32 bit field numeric field.
   */
  private static class IntIntStrategy extends IntFieldValueStrategy {

    private final String field;
    private NumericDocValues minMaxVals;
    private IntIntDynamicMap testValues;
    private IntCompare comp;
    private int nullCompVal;

    private int index=-1;

    public IntIntStrategy(int maxDoc,
                          int size,
                          String collapseField,
                          int nullPolicy,
                          GroupHeadSelector groupHeadSelector,
                          boolean needsScores,
                          BoostedDocsCollector boostedDocsCollector) throws IOException {

      super(maxDoc, size, collapseField, nullPolicy, needsScores, boostedDocsCollector);
      this.field = groupHeadSelector.selectorText;
      this.testValues = new IntIntDynamicMap(size, 0);

      assert GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type);

      if (GroupHeadSelectorType.MAX.equals(groupHeadSelector.type)) {
        comp = new MaxIntComp();
        this.nullCompVal = Integer.MIN_VALUE;
      } else {
        comp = new MinIntComp();
        this.nullCompVal = Integer.MAX_VALUE;
      }
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.minMaxVals = DocValues.getNumeric(context.reader(), this.field);
    }
    private int advanceAndGetCurrentVal(int contextDoc) throws IOException {
      if (minMaxVals.advanceExact(contextDoc)) {
        return (int) minMaxVals.longValue();
      } // else...
      return 0; 
    }
    public void collapse(int collapseKey, int contextDoc, int globalDoc) throws IOException {
      final int currentVal = advanceAndGetCurrentVal(contextDoc);

      final int idx;
      if((idx = cmap.indexOf(collapseKey)) >= 0) {
        int pointer = cmap.indexGet(idx);
        if(comp.test(currentVal, testValues.get(pointer))) {
          testValues.put(pointer, currentVal);
          docs.put(pointer, globalDoc);
          if(needsScores) {
            scores.put(pointer, scorer.score());
          }
        }
      } else {
        ++index;
        cmap.put(collapseKey, index);
        testValues.put(index, currentVal);
        docs.put(index, globalDoc);
        if(needsScores) {
          scores.put(index, scorer.score());
        }
      }
    }
    public void collapseNullGroup(int contextDoc, int globalDoc) throws IOException {
      assert NullPolicy.IGNORE.getCode() != this.nullPolicy;
      
      final int currentVal = advanceAndGetCurrentVal(contextDoc);
      if (this.nullPolicy == NullPolicy.COLLAPSE.getCode()) {
        if(comp.test(currentVal, nullCompVal)) {
          nullCompVal = currentVal;
          nullDoc = globalDoc;
          if(needsScores) {
            nullScore = scorer.score();
          }
        }
      } else if(this.nullPolicy == NullPolicy.EXPAND.getCode()) {
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
    private IntFloatDynamicMap testValues;
    private FloatCompare comp;
    private float nullCompVal;

    private int index=-1;

    public IntFloatStrategy(int maxDoc,
                            int size,
                            String collapseField,
                            int nullPolicy,
                            GroupHeadSelector groupHeadSelector,
                            boolean needsScores,
                            BoostedDocsCollector boostedDocsCollector) throws IOException {

      super(maxDoc, size, collapseField, nullPolicy, needsScores, boostedDocsCollector);
      this.field = groupHeadSelector.selectorText;
      this.testValues = new IntFloatDynamicMap(size, 0.0f);

      assert GroupHeadSelectorType.MIN_MAX.contains(groupHeadSelector.type);

      if (GroupHeadSelectorType.MAX.equals(groupHeadSelector.type)) {
        comp = new MaxFloatComp();
        this.nullCompVal = -Float.MAX_VALUE;
      } else {
        comp = new MinFloatComp();
        this.nullCompVal = Float.MAX_VALUE;
      }
    }

    public void setNextReader(LeafReaderContext context) throws IOException {
      this.minMaxVals = DocValues.getNumeric(context.reader(), this.field);
    }
    private float advanceAndGetCurrentVal(int contextDoc) throws IOException {
      if (minMaxVals.advanceExact(contextDoc)) {
        return Float.intBitsToFloat((int) minMaxVals.longValue());
      } // else...
      return Float.intBitsToFloat(0);
    }

    public void collapse(int collapseKey, int contextDoc, int globalDoc) throws IOException {
      final float currentVal = advanceAndGetCurrentVal(contextDoc);

      final int idx;
      if((idx = cmap.indexOf(collapseKey)) >= 0) {
        int pointer = cmap.indexGet(idx);
        if(comp.test(currentVal, testValues.get(pointer))) {
          testValues.put(pointer, currentVal);
          docs.put(pointer, globalDoc);
          if(needsScores) {
            scores.put(pointer, scorer.score());
          }
        }
      } else {
        ++index;
        cmap.put(collapseKey, index);
        testValues.put(index, currentVal);
        docs.put(index, globalDoc);
        if(needsScores) {
          scores.put(index, scorer.score());
        }
      }
    }
    public void collapseNullGroup(int contextDoc, int globalDoc) throws IOException {
      assert NullPolicy.IGNORE.getCode() != this.nullPolicy;
      final float currentVal = advanceAndGetCurrentVal(contextDoc);
      if(this.nullPolicy == NullPolicy.COLLAPSE.getCode()) {
        if(comp.test(currentVal, nullCompVal)) {
          nullCompVal = currentVal;
          nullDoc = globalDoc;
          if(needsScores) {
            nullScore = scorer.score();
          }
        }
      } else if(this.nullPolicy == NullPolicy.EXPAND.getCode()) {
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
    private IntFloatDynamicMap testValues;
    private float nullCompVal;

    private ValueSource valueSource;
    private FunctionValues functionValues;
    @SuppressWarnings({"rawtypes"})
    private Map rcontext;
    private final CollapseScore collapseScore = new CollapseScore();
    private int index=-1;
    private boolean needsScores4Collapsing;

    public IntValueSourceStrategy(int maxDoc,
                                  int size,
                                  String collapseField,
                                  int nullPolicy,
                                  GroupHeadSelector groupHeadSelector,
                                  boolean needsScores4Collapsing,
                                  boolean needsScores,
                                  BoostedDocsCollector boostedDocsCollector,
                                  FunctionQuery funcQuery,
                                  IndexSearcher searcher) throws IOException {

      super(maxDoc, size, collapseField, nullPolicy, needsScores, boostedDocsCollector);

      this.needsScores4Collapsing = needsScores4Collapsing;
      this.testValues = new IntFloatDynamicMap(size, 0.0f);

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
    }

    @SuppressWarnings({"unchecked"})
    public void setNextReader(LeafReaderContext context) throws IOException {
      functionValues = this.valueSource.getValues(rcontext, context);
    }
    private float computeScoreIfNeeded4Collapse() throws IOException {
      if (needsScores4Collapsing) {
        this.collapseScore.score = scorer.score();
        return this.collapseScore.score;
      } // else...
      return 0F;
    }
    public void collapse(int collapseKey, int contextDoc, int globalDoc) throws IOException {
      
      float score = computeScoreIfNeeded4Collapse();
      final float currentVal = functionValues.floatVal(contextDoc);

      final int idx;
      if((idx = cmap.indexOf(collapseKey)) >= 0) {
        int pointer = cmap.indexGet(idx);
        if(comp.test(currentVal, testValues.get(pointer))) {
          testValues.put(pointer, currentVal);
          docs.put(pointer, globalDoc);
          if(needsScores){
            if (!needsScores4Collapsing) {
              score = scorer.score();
            }
            scores.put(pointer, score);
          }
        }
      } else {
        ++index;
        cmap.put(collapseKey, index);
        docs.put(index, globalDoc);
        testValues.put(index, currentVal);
        if(needsScores) {
          if (!needsScores4Collapsing) {
            score = scorer.score();
          }
          scores.put(index, score);
        }
      }
    }
    public void collapseNullGroup(int contextDoc, int globalDoc) throws IOException {
      assert NullPolicy.IGNORE.getCode() != this.nullPolicy;
      
      float score = computeScoreIfNeeded4Collapse();
      final float currentVal = functionValues.floatVal(contextDoc);

      if(this.nullPolicy == NullPolicy.COLLAPSE.getCode()) {
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
      } else if(this.nullPolicy == NullPolicy.EXPAND.getCode()) {
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
                               int nullPolicy,
                               GroupHeadSelector groupHeadSelector,
                               boolean needsScores4Collapsing,
                               boolean needsScores,
                               BoostedDocsCollector boostedDocsCollector,
                               SortSpec sortSpec,
                               IndexSearcher searcher) throws IOException {

      super(maxDoc, size, collapseField, nullPolicy, needsScores, boostedDocsCollector);
      this.needsScores4Collapsing = needsScores4Collapsing;

      assert GroupHeadSelectorType.SORT.equals(groupHeadSelector.type);

      this.sortSpec = sortSpec;
      this.sort = rewriteSort(sortSpec, searcher);
      this.compareState = new SortFieldsCompare(sort.getSort(), size);
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
    
    private float computeScoreIfNeeded4Collapse() throws IOException {
      return needsScores4Collapsing ? scorer.score() : 0F;
    }

    public void collapse(int collapseKey, int contextDoc, int globalDoc) throws IOException {
      float score = computeScoreIfNeeded4Collapse();

      final int idx;
      if ((idx = cmap.indexOf(collapseKey)) >= 0) {
        // we've seen this collapseKey before, test to see if it's a new group leader
        int pointer = cmap.indexGet(idx);
        if (compareState.testAndSetGroupValues(pointer, contextDoc)) {
          docs.put(pointer, globalDoc);
          if (needsScores) {
            if (!needsScores4Collapsing) {
              score = scorer.score();
            }
            scores.put(pointer, score);
          }
        }
      } else {
        // we've never seen this collapseKey before, treat it as group head for now
        ++index;
        cmap.put(collapseKey, index);
        docs.put(index, globalDoc);
        compareState.setGroupValues(index, contextDoc);
        if(needsScores) {
          if (!needsScores4Collapsing) {
            score = scorer.score();
          }
          scores.put(index, score);
        }
      }
    }

    public void collapseNullGroup(int contextDoc, int globalDoc) throws IOException {
      assert NullPolicy.IGNORE.getCode() != this.nullPolicy;
      
      float score = computeScoreIfNeeded4Collapse();

      if(this.nullPolicy == NullPolicy.COLLAPSE.getCode()) {
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
      } else if(this.nullPolicy == NullPolicy.EXPAND.getCode()) {
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

  /**
   * Helper class for dealing with boosted docs, which always get collected 
   * (even if there is more then one in a group) and suppress any non-boosted 
   * docs from being collected from their group (even if they should be based 
   * on the group head selectors)
   *
   * NOTE: collect methods must be called in increasing globalDoc order
   */
  private static class BoostedDocsCollector {
    private final IntIntHashMap boostDocsMap;
    private final int[] sortedGlobalDocIds;
    private final boolean hasBoosts;
    
    private final IntArrayList boostedKeys = new IntArrayList();
    private final IntArrayList boostedDocs = new IntArrayList();;
    private boolean boostedNullGroup = false;
    private final MergeBoost boostedDocsIdsIter;

    public static BoostedDocsCollector build(final IntIntHashMap boostDocsMap) {
      if (null != boostDocsMap && ! boostDocsMap.isEmpty()) {
        return new BoostedDocsCollector(boostDocsMap);
      }

      // else: No-Op impl (short circut default impl)....
      return new BoostedDocsCollector(new IntIntHashMap()) {
        @Override
        public boolean collectIfBoosted(int groupKey, int globalDoc) {
          return false;
        }
        @Override
        public boolean collectInNullGroupIfBoosted(int globalDoc) {
          return false;
        }
        @Override
        public void purgeGroupsThatHaveBoostedDocs(final FixedBitSet collapsedSet,
                                                   final IntProcedure removeGroupKey,
                                                   final Runnable resetNullGroupHead) {
          return;
        }
      };
    }
    
    private BoostedDocsCollector(final IntIntHashMap boostDocsMap) {
      this.boostDocsMap = boostDocsMap;
      this.hasBoosts = ! boostDocsMap.isEmpty();
      sortedGlobalDocIds = new int[boostDocsMap.size()];
      Iterator<IntIntCursor> it = boostDocsMap.iterator();
      int index = -1;
      while(it.hasNext()) {
        IntIntCursor cursor = it.next();
        sortedGlobalDocIds[++index] = cursor.key;
      }
      
      Arrays.sort(sortedGlobalDocIds);
      boostedDocsIdsIter = getMergeBoost();
    }

    /** True if there are any requested boosts (regardless of wether any have been collected) */
    public boolean hasBoosts() {
      return hasBoosts;
    }
    
    /**
     * Returns a brand new MergeBoost instance listing all requested boosted docs 
     */
    public MergeBoost getMergeBoost() {
      return new MergeBoost(sortedGlobalDocIds);
    }

    /** 
     * @return true if doc is boosted and has (now) been collected
     */
    public boolean collectIfBoosted(int groupKey, int globalDoc) {
      if (boostedDocsIdsIter.boost(globalDoc)) {
        this.boostedDocs.add(globalDoc);
        this.boostedKeys.add(groupKey);
        return true;
      }
      return false;
    }
    
    /** 
     * @return true if doc is boosted and has (now) been collected
     */
    public boolean collectInNullGroupIfBoosted(int globalDoc) {
      if (boostedDocsIdsIter.boost(globalDoc)) {
        this.boostedDocs.add(globalDoc);
        this.boostedNullGroup = true;
        return true;
      }
      return false;
    }

    /** 
     * Kludgy API neccessary to deal with diff collectors/strategies using diff
     * data structs for tracking collapse keys...
     */
    public void purgeGroupsThatHaveBoostedDocs(final FixedBitSet collapsedSet,
                                               final IntProcedure removeGroupKey,
                                               final Runnable resetNullGroupHead) {
      // Add the (collected) boosted docs to the collapsedSet
      boostedDocs.forEach(new IntProcedure() {
        public void apply(int globalDoc) {
          collapsedSet.set(globalDoc);
        }
      });
      // Remove any group heads that are in the same groups as (collected) boosted documents.
      boostedKeys.forEach(removeGroupKey);
      if (boostedNullGroup) {
        // If we're using IGNORE then no (matching) null docs were collected (by caller)
        // If we're using EXPAND then all (matching) null docs were already collected (by us)
        //   ...and that's *good* because each is treated like it's own group, our boosts don't matter
        // We only have to worry about removing null docs when using COLLAPSE, in which case any boosted null doc
        // means we clear the group head of the null group..
        resetNullGroupHead.run();
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
   * This structure wraps (and semi-emulates) the {@link SortFieldsCompare} functionality/API
   * for "block" based group collection, where we only ever need a single group in memory at a time
   * As a result, it's API has a smaller surface area...
   */
  private static class BlockBasedSortFieldsCompare {
    /** 
     * this will always have a numGroups of '0' and we will (ab)use the 'null' group methods for tracking 
     * and comparison as we collect docs (since we only ever consider one group at a time)
     */
    final private SortFieldsCompare inner;
    public BlockBasedSortFieldsCompare(final SortField[] sorts) {
      this.inner = new SortFieldsCompare(sorts, 0);
    }
    public void setNextReader(LeafReaderContext context) throws IOException {
      inner.setNextReader(context);
    }
    public void setScorer(Scorable s) throws IOException {
      inner.setScorer(s);
    }
    /** @see SortFieldsCompare#setGroupValues */
    public void setGroupValues(int contextDoc) throws IOException {
      inner.setNullGroupValues(contextDoc);
    }
    /** @see SortFieldsCompare#testAndSetGroupValues */
    public boolean testAndSetGroupValues(int contextDoc) throws IOException {
      return inner.testAndSetNullGroupValues(contextDoc);
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
    @SuppressWarnings({"rawtypes"})
    final private FieldComparator[] fieldComparators;
    final private LeafFieldComparator[] leafFieldComparators;

    private Object[][] groupHeadValues; // growable
    final private Object[] nullGroupValues;

    /**
     * Constructs an instance based on the the (raw, un-rewritten) SortFields to be used,
     * and an initial number of expected groups (will grow as needed).
     */
    @SuppressWarnings({"rawtypes"})
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
      if (collapseKey >= groupHeadValues.length) {
        grow(collapseKey + 1);
      }
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
      if (collapseKey >= groupHeadValues.length) {
        grow(collapseKey + 1);
      }
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
    @SuppressWarnings({"unchecked", "rawtypes"})
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

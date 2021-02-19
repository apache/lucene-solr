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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ExpandParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.search.CollapsingQParserPlugin;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpecParsing;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * The ExpandComponent is designed to work with the CollapsingPostFilter.
 * The CollapsingPostFilter collapses a result set on a field.
 * <p>
 * The ExpandComponent expands the collapsed groups for a single page.
 * When multiple collapse groups are specified then, the field is chosen from collapse group with min cost.
 * If the cost are equal then, the field is chosen from first collapse group.
 * <p>
 * http parameters:
 * <p>
 * expand=true <br>
 * expand.rows=5 <br>
 * expand.sort=field asc|desc<br>
 * expand.q=*:* (optional, overrides the main query)<br>
 * expand.fq=type:child (optional, overrides the main filter queries)<br>
 * expand.field=field (mandatory, if the not used with the CollapsingQParserPlugin. This is given higher priority when both are present)<br>
 */
public class ExpandComponent extends SearchComponent implements PluginInfoInitialized, SolrCoreAware {
  public static final String COMPONENT_NAME = "expand";
  private static final int finishingStage = ResponseBuilder.STAGE_GET_FIELDS;
  private PluginInfo info = PluginInfo.EMPTY_INFO;

  @Override
  public void init(PluginInfo info) {
    this.info = info;
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    if (rb.req.getParams().getBool(ExpandParams.EXPAND, false)) {
      if (rb.req.getParams().getBool(GroupParams.GROUP, false)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can not use expand with Grouping enabled");
      }
      rb.doExpand = true;
    }
  }

  @Override
  public void inform(SolrCore core) {

  }

  @SuppressWarnings("unchecked")
  @Override
  public void process(ResponseBuilder rb) throws IOException {

    if (!rb.doExpand) {
      return;
    }

    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams();

    String field = params.get(ExpandParams.EXPAND_FIELD);
    String hint = null;
    if (field == null) {
      List<Query> filters = rb.getFilters();
      if (filters != null) {
        int cost = Integer.MAX_VALUE;
        for (Query q : filters) {
          if (q instanceof CollapsingQParserPlugin.CollapsingPostFilter) {
            CollapsingQParserPlugin.CollapsingPostFilter cp = (CollapsingQParserPlugin.CollapsingPostFilter) q;
            // if there are multiple collapse pick the low cost one
            // if cost are equal then first one is picked
            if (cp.getCost() < cost) {
              cost = cp.getCost();
              field = cp.getField();
              hint = cp.hint;
            }
          }
        }
      }
    }

    if (field == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "missing expand field");
    }

    String sortParam = params.get(ExpandParams.EXPAND_SORT);
    String[] fqs = params.getParams(ExpandParams.EXPAND_FQ);
    String qs = params.get(ExpandParams.EXPAND_Q);
    int limit = params.getInt(ExpandParams.EXPAND_ROWS, 5);

    Sort sort = null;

    if (sortParam != null) {
      sort = SortSpecParsing.parseSortSpec(sortParam, rb.req).getSort();
    }

    final Query query;
    List<Query> newFilters = new ArrayList<>();
    try {
      if (qs == null) {
        query = rb.getQuery();
      } else {
        QParser parser = QParser.getParser(qs, req);
        query = parser.getQuery();
      }

      if (fqs == null) {
        List<Query> filters = rb.getFilters();
        if (filters != null) {
          for (Query q : filters) {
            if (!(q instanceof CollapsingQParserPlugin.CollapsingPostFilter)) {
              newFilters.add(q);
            }
          }
        }
      } else {
        for (String fq : fqs) {
          if (StringUtils.isNotBlank(fq) && !fq.equals("*:*")) {
            QParser fqp = QParser.getParser(fq, req);
            newFilters.add(fqp.getQuery());
          }
        }
      }
    } catch (SyntaxError e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }

    SolrIndexSearcher searcher = req.getSearcher();
    LeafReader reader = searcher.getSlowAtomicReader();

    SchemaField schemaField = searcher.getSchema().getField(field);
    FieldType fieldType = schemaField.getType();

    SortedDocValues values = null;

    if(fieldType instanceof StrField) {
      //Get The Top Level SortedDocValues
      if(CollapsingQParserPlugin.HINT_TOP_FC.equals(hint)) {
        @SuppressWarnings("resource")
        LeafReader uninvertingReader = CollapsingQParserPlugin.getTopFieldCacheReader(searcher, field);
        values = uninvertingReader.getSortedDocValues(field);
      } else {
        values = DocValues.getSorted(reader, field);
      }
    } else if (fieldType.getNumberType() == null) {
      // possible if directly expand.field is specified
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Expand not supported for fieldType:'" + fieldType.getTypeName() +"'");
    }

    FixedBitSet groupBits = null;
    LongHashSet groupSet = null;
    DocList docList = rb.getResults().docList;
    IntHashSet collapsedSet = new IntHashSet(docList.size() * 2);

    //Gather the groups for the current page of documents
    DocIterator idit = docList.iterator();
    int[] globalDocs = new int[docList.size()];
    int docsIndex = -1;
    while (idit.hasNext()) {
      globalDocs[++docsIndex] = idit.nextDoc();
    }

    Arrays.sort(globalDocs);
    Query groupQuery = null;

    /*
    * This code gathers the group information for the current page.
    */
    List<LeafReaderContext> contexts = searcher.getTopReaderContext().leaves();

    if(contexts.size() == 0) {
      //When no context is available we can skip the expanding
      return;
    }

    boolean nullGroupOnCurrentPage = false;
    int currentContext = 0;
    int currentDocBase = contexts.get(currentContext).docBase;
    int nextDocBase = (currentContext+1)<contexts.size() ? contexts.get(currentContext+1).docBase : Integer.MAX_VALUE;
    IntObjectHashMap<BytesRef> ordBytes = null;
    if(values != null) {
      groupBits = new FixedBitSet(values.getValueCount());
      OrdinalMap ordinalMap = null;
      SortedDocValues[] sortedDocValues = null;
      LongValues segmentOrdinalMap = null;
      SortedDocValues currentValues = null;
      if(values instanceof MultiDocValues.MultiSortedDocValues) {
        ordinalMap = ((MultiDocValues.MultiSortedDocValues)values).mapping;
        sortedDocValues = ((MultiDocValues.MultiSortedDocValues)values).values;
        currentValues = sortedDocValues[currentContext];
        segmentOrdinalMap = ordinalMap.getGlobalOrds(currentContext);
      }

      ordBytes = new IntObjectHashMap<>();

      for(int i=0; i<globalDocs.length; i++) {
        int globalDoc = globalDocs[i];
        while(globalDoc >= nextDocBase) {
          currentContext++;
          currentDocBase = contexts.get(currentContext).docBase;
          nextDocBase = (currentContext+1) < contexts.size() ? contexts.get(currentContext+1).docBase : Integer.MAX_VALUE;
          if(ordinalMap != null) {
            currentValues = sortedDocValues[currentContext];
            segmentOrdinalMap = ordinalMap.getGlobalOrds(currentContext);
          }
        }
        collapsedSet.add(globalDoc);
        int contextDoc = globalDoc - currentDocBase;
        if(ordinalMap != null) {
          if (contextDoc > currentValues.docID()) {
            currentValues.advance(contextDoc);
          }
          if (contextDoc == currentValues.docID()) {
            int contextOrd = currentValues.ordValue();
            int ord = (int)segmentOrdinalMap.get(contextOrd);
            if (!groupBits.getAndSet(ord)) {
              BytesRef ref = currentValues.lookupOrd(contextOrd);
              ordBytes.put(ord, BytesRef.deepCopyOf(ref));
            }
          } else {
            nullGroupOnCurrentPage = true;
          }
          
        } else {
          if (globalDoc > values.docID()) {
            values.advance(globalDoc);
          }
          if (globalDoc == values.docID()) {
            int ord = values.ordValue();
            if (!groupBits.getAndSet(ord)) {
              BytesRef ref = values.lookupOrd(ord);
              ordBytes.put(ord, BytesRef.deepCopyOf(ref));
            }
          } else {
            nullGroupOnCurrentPage = true;
          }
        }
      }

      int count = ordBytes.size();
      if(count > 0 && count < 200) {
        groupQuery = getGroupQuery(field, count, ordBytes);
      }
    } else {
      groupSet = new LongHashSet(docList.size());
      NumericDocValues collapseValues = contexts.get(currentContext).reader().getNumericDocValues(field);
      for(int i=0; i<globalDocs.length; i++) {
        int globalDoc = globalDocs[i];
        while(globalDoc >= nextDocBase) {
          currentContext++;
          currentDocBase = contexts.get(currentContext).docBase;
          nextDocBase = currentContext+1 < contexts.size() ? contexts.get(currentContext+1).docBase : Integer.MAX_VALUE;
          collapseValues = contexts.get(currentContext).reader().getNumericDocValues(field);
        }
        collapsedSet.add(globalDoc);
        int contextDoc = globalDoc - currentDocBase;
        int valueDocID = collapseValues.docID();
        if (valueDocID < contextDoc) {
          valueDocID = collapseValues.advance(contextDoc);
        }
        if (valueDocID == contextDoc) {
          final long value = collapseValues.longValue();
          groupSet.add(value);
        } else {
          nullGroupOnCurrentPage = true;
        }
      }

      int count = groupSet.size();
      if(count > 0 && count < 200) {
        if (fieldType.isPointField()) {
          groupQuery = getPointGroupQuery(schemaField, count, groupSet);
        } else {
          groupQuery = getGroupQuery(field, fieldType, count, groupSet);
        }
      }
    }

    final boolean expandNullGroup =
      params.getBool(ExpandParams.EXPAND_NULL, false) &&
      // Our GroupCollector can typically ignore nulls (and the user's nullGroup param) unless the
      // current page had any - but if expand.q was specified, current page doesn't mater: We
      // need look for nulls if the user asked us to because we don't know what the expand.q will match
      (nullGroupOnCurrentPage || (null != query));

    
    if (expandNullGroup && null != groupQuery) {
      // we need to also consider docs w/o a field value 
      final BooleanQuery.Builder inner = new BooleanQuery.Builder();
      inner.add(fieldType.getExistenceQuery(null, schemaField), BooleanClause.Occur.MUST_NOT);
      inner.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
      final BooleanQuery.Builder outer = new BooleanQuery.Builder();
      outer.add(inner.build(), BooleanClause.Occur.SHOULD);
      outer.add(groupQuery, BooleanClause.Occur.SHOULD);
      groupQuery = outer.build();
    }
    
    Collector collector;
    if (sort != null)
      sort = sort.rewrite(searcher);


    GroupCollector groupExpandCollector = null;

    if(values != null) {
      //Get The Top Level SortedDocValues again so we can re-iterate:
      if(CollapsingQParserPlugin.HINT_TOP_FC.equals(hint)) {
        @SuppressWarnings("resource")
        LeafReader uninvertingReader = CollapsingQParserPlugin.getTopFieldCacheReader(searcher, field);
        values = uninvertingReader.getSortedDocValues(field);
      } else {
        values = DocValues.getSorted(reader, field);
      }

      groupExpandCollector = new GroupExpandCollector(limit, sort, query, expandNullGroup,
                                                      fieldType, ordBytes,
                                                      values, groupBits, collapsedSet);
    } else {
      groupExpandCollector = new NumericGroupExpandCollector(limit, sort, query, expandNullGroup,
                                                             fieldType, ordBytes,
                                                             field, groupSet, collapsedSet);
    }

    if(groupQuery !=  null) {
      //Limits the results to documents that are in the same group as the documents in the page.
      newFilters.add(groupQuery);
    }

    SolrIndexSearcher.ProcessedFilter pfilter = searcher.getProcessedFilter(null, newFilters);
    if (pfilter.postFilter != null) {
      pfilter.postFilter.setLastDelegate(groupExpandCollector);
      collector = pfilter.postFilter;
    } else {
      collector = groupExpandCollector;
    }

    searcher.search(QueryUtils.combineQueryAndFilter(query, pfilter.filter), collector);

    rb.rsp.add("expanded", groupExpandCollector.getGroups(searcher, rb.rsp.getReturnFields()));
  }

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    if (rb.doExpand && rb.stage < finishingStage) {
      return finishingStage;
    }
    return ResponseBuilder.STAGE_DONE;
  }
    
  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false)) return;
    if (!rb.onePassDistributedQuery && (sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) == 0) {
      sreq.params.set(COMPONENT_NAME, "false");
    } else {
      sreq.params.set(COMPONENT_NAME, "true");
    }
  }


  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {

    if (!rb.doExpand) {
      return;
    }
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
      SolrQueryRequest req = rb.req;
      NamedList expanded = (NamedList) req.getContext().get("expanded");
      if (expanded == null) {
        expanded = new SimpleOrderedMap();
        req.getContext().put("expanded", expanded);
      }

      for (ShardResponse srsp : sreq.responses) {
        NamedList response = srsp.getSolrResponse().getResponse();
        NamedList ex = (NamedList) response.get("expanded");
        for (int i=0; i<ex.size(); i++) {
          String name = ex.getName(i);
          SolrDocumentList val = (SolrDocumentList) ex.getVal(i);
          expanded.add(name, val);
        }
      }
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void finishStage(ResponseBuilder rb) {

    if (!rb.doExpand) {
      return;
    }

    if (rb.stage != finishingStage) {
      return;
    }

    NamedList expanded = (NamedList) rb.req.getContext().get("expanded");
    if (expanded == null) {
      expanded = new SimpleOrderedMap();
    }

    rb.rsp.add("expanded", expanded);
  }

  private static class GroupExpandCollector extends GroupCollector {
    private final SortedDocValues docValues;
    private final OrdinalMap ordinalMap;
    private final MultiDocValues.MultiSortedDocValues multiSortedDocValues;

    private final LongObjectMap<Collector> groups;
    private final FixedBitSet groupBits;
    private final IntHashSet collapsedSet;

    public GroupExpandCollector(int limit, Sort sort, Query query, boolean expandNulls,
                                FieldType fieldType, IntObjectHashMap<BytesRef> ordBytes,
                                SortedDocValues docValues, FixedBitSet groupBits, IntHashSet collapsedSet) throws IOException {
      super(limit, sort, query, expandNulls, fieldType, ordBytes);

      // groupBits.cardinality() is more expensive then collapsedSet.size() which is adequate for an upper bound
      this.groups = new LongObjectHashMap<>(collapsedSet.size());
      DocIdSetIterator iterator = new BitSetIterator(groupBits, 0); // cost is not useful here
      int group;
      while ((group = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        groups.put(group, getCollector());
      }

      this.collapsedSet = collapsedSet;
      this.groupBits = groupBits;
      this.docValues = docValues;
      if(docValues instanceof MultiDocValues.MultiSortedDocValues) {
        this.multiSortedDocValues = (MultiDocValues.MultiSortedDocValues)docValues;
        this.ordinalMap = multiSortedDocValues.mapping;
      } else {
        this.multiSortedDocValues = null;
        this.ordinalMap = null;
      }
    }

    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      final int docBase = context.docBase;

      final boolean useOrdinalMapping = (null != ordinalMap);
      final SortedDocValues segmentValues = useOrdinalMapping ? this.multiSortedDocValues.values[context.ord] : null;
      final LongValues segmentOrdinalMap = useOrdinalMapping ? this.ordinalMap.getGlobalOrds(context.ord) : null;

      final LeafCollector leafNullGroupCollector = expandNullGroup ? nullGroupCollector.getLeafCollector(context) : null;
      final LongObjectMap<LeafCollector> leafCollectors = new LongObjectHashMap<>();
      for (LongObjectCursor<Collector> entry : groups) {
        leafCollectors.put(entry.key, entry.value.getLeafCollector(context));
      }
      return new LeafCollector() {

        @Override
        public void setScorer(Scorable scorer) throws IOException {
          for (ObjectCursor<LeafCollector> c : leafCollectors.values()) {
            c.value.setScorer(scorer);
          }
          if (expandNullGroup) {
            leafNullGroupCollector.setScorer(scorer);
          }
        }

        @Override
        public void collect(int docId) throws IOException {
          int globalDoc = docId + docBase;

          if (collapsedSet.contains(globalDoc)) {
            return; // this doc is already a group head
          }
          
          int ord = -1;
          if (useOrdinalMapping) {
            if (docId > segmentValues.docID()) {
              segmentValues.advance(docId);
            }
            if (docId == segmentValues.docID()) {
              ord = (int)segmentOrdinalMap.get(segmentValues.ordValue());
            } else {
              ord = -1;
            }
          } else {
            if (docValues.advanceExact(globalDoc)) {
              ord = docValues.ordValue();
            } else {
              ord = -1;
            }
          }
          
          if (ord > -1) {
            if (groupBits.get(ord)) {
              LeafCollector c = leafCollectors.get(ord);
              c.collect(docId);
            }
          } else if (expandNullGroup) {
            leafNullGroupCollector.collect(docId);
          }
        }
      };
    }

    @Override
    protected LongObjectMap<Collector> getGroups() {
      return groups;
    }
  }

  private static class NumericGroupExpandCollector extends GroupCollector {

    private final String field;
    private final LongObjectHashMap<Collector> groups;
    private final IntHashSet collapsedSet;

    public NumericGroupExpandCollector(int limit, Sort sort, Query query, boolean expandNulls,
                                       FieldType fieldType, IntObjectHashMap<BytesRef> ordBytes,
                                       String field, LongHashSet groupSet, IntHashSet collapsedSet) throws IOException {
      super(limit, sort, query, expandNulls, fieldType, ordBytes);
      
      this.groups = new LongObjectHashMap<>(groupSet.size());
      for (LongCursor cursor : groupSet) {
        groups.put(cursor.value, getCollector());
      }

      this.field = field;
      this.collapsedSet = collapsedSet;
    }

    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      final int docBase = context.docBase;
      
      final NumericDocValues docValues = context.reader().getNumericDocValues(this.field);
      final LeafCollector leafNullGroupCollector = expandNullGroup ? nullGroupCollector.getLeafCollector(context) : null;
      final LongObjectHashMap<LeafCollector> leafCollectors = new LongObjectHashMap<>();
      for (LongObjectCursor<Collector> entry : groups) {
        leafCollectors.put(entry.key, entry.value.getLeafCollector(context));
      }

      return new LeafCollector() {

        @Override
        public void setScorer(Scorable scorer) throws IOException {
          for (ObjectCursor<LeafCollector> c : leafCollectors.values()) {
            c.value.setScorer(scorer);
          }
          if (expandNullGroup) {
            leafNullGroupCollector.setScorer(scorer);
          }
        }

        @Override
        public void collect(int docId) throws IOException {
          if (docValues.advanceExact(docId)) {
            final long value = docValues.longValue();
            final int index = leafCollectors.indexOf(value);
            if (index >= 0 && !collapsedSet.contains(docId + docBase)) {
              leafCollectors.indexGet(index).collect(docId);
            }
          } else if (expandNullGroup && !collapsedSet.contains(docId + docBase)) {
            leafNullGroupCollector.collect(docId);
          }
        }
      };
    }

    @Override
    protected LongObjectHashMap<Collector> getGroups() {
      return groups;
    }

  }

  private static abstract class GroupCollector implements Collector {
    
    protected final int limit;
    protected final Sort sort;
    protected final Query query;
    protected final boolean expandNullGroup;
    protected final FieldType fieldType;
    protected final IntObjectHashMap<BytesRef> ordBytes;
    
    protected final Collector nullGroupCollector;
    
    public GroupCollector(int limit, Sort sort, Query query, boolean expandNullGroup,
                          FieldType fieldType, IntObjectHashMap<BytesRef> ordBytes) throws IOException {
      this.limit = limit;
      this.sort = sort;
      this.query = query;
      this.expandNullGroup = expandNullGroup;
      this.fieldType = fieldType;
      this.ordBytes = ordBytes;
      this.nullGroupCollector = expandNullGroup ? getCollector() : null;
    }
    
    protected abstract LongObjectMap<Collector> getGroups();

    public final SimpleOrderedMap<DocSlice> getGroups(SolrIndexSearcher searcher, ReturnFields returnFields) throws IOException {
      
      final SimpleOrderedMap<DocSlice> outMap = new SimpleOrderedMap<>();
      final CharsRefBuilder charsRef = new CharsRefBuilder();
      for (LongObjectCursor<Collector> cursor : getGroups()) {
        final long groupValue = cursor.key;
        final DocSlice slice = collectorToDocSlice(cursor.value, searcher, returnFields);
        if (null != slice) {
          addGroupSliceToOutputMap(outMap, charsRef, groupValue, slice);
        }
      }
      if (expandNullGroup) {
        assert null != nullGroupCollector;
        final DocSlice nullGroup = collectorToDocSlice(nullGroupCollector, searcher, returnFields);
        if (null != nullGroup) {
          outMap.add(null, nullGroup);
        }
      }
      return outMap;
    }
    
    private DocSlice collectorToDocSlice(Collector groupCollector, SolrIndexSearcher searcher, ReturnFields returnFields) throws IOException {
      if (groupCollector instanceof TopDocsCollector) {
        TopDocsCollector<?> topDocsCollector = TopDocsCollector.class.cast(groupCollector);
        TopDocs topDocs = topDocsCollector.topDocs();
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        if (scoreDocs.length > 0) {
          if (returnFields.wantsScore() && sort != null) {
            TopFieldCollector.populateScores(scoreDocs, searcher, query);
          }
          int[] docs = new int[scoreDocs.length];
          float[] scores = new float[scoreDocs.length];
          for (int i = 0; i < docs.length; i++) {
            ScoreDoc scoreDoc = scoreDocs[i];
            docs[i] = scoreDoc.doc;
            scores[i] = scoreDoc.score;
          }
          assert topDocs.totalHits.relation == TotalHits.Relation.EQUAL_TO;
          return new DocSlice(0, docs.length, docs, scores, topDocs.totalHits.value, Float.NaN, TotalHits.Relation.EQUAL_TO);
        }
      } else {
        int totalHits = ((TotalHitCountCollector) groupCollector).getTotalHits();
        if (totalHits > 0) {
          return new DocSlice(0, 0, null, null, totalHits, 0, TotalHits.Relation.EQUAL_TO);
        }
      }
      return null;
    }
    
    private void addGroupSliceToOutputMap(NamedList<DocSlice> outMap, CharsRefBuilder charsRef,
                                          long groupValue, DocSlice slice) {
      if(fieldType instanceof StrField) {
        final BytesRef bytesRef = ordBytes.get((int)groupValue);
        fieldType.indexedToReadable(bytesRef, charsRef);
        String group = charsRef.toString();
        outMap.add(group, slice);
      } else {
        outMap.add(numericToString(fieldType, groupValue), slice);
      }
    }
    
    @Override
    public ScoreMode scoreMode() {
      final LongObjectMap<Collector> groups = getGroups();
      if (groups.isEmpty()) {
        return ScoreMode.COMPLETE; // doesn't matter?
      } else {
        return groups.iterator().next().value.scoreMode(); // we assume all the collectors should have the same nature
      }
    }

    protected final Collector getCollector()  throws IOException {
      Collector collector;
      if (limit == 0) {
        collector = new TotalHitCountCollector();
      } else if (sort == null) {
        collector = TopScoreDocCollector.create(limit, Integer.MAX_VALUE);
      } else {
        collector = TopFieldCollector.create(sort, limit, Integer.MAX_VALUE);
      }
      return collector;
    }
  }

  private Query getGroupQuery(String fname,
                           FieldType ft,
                           int size,
                           LongHashSet groupSet) {

    BytesRef[] bytesRefs = new BytesRef[size];
    int index = -1;
    BytesRefBuilder term = new BytesRefBuilder();
    Iterator<LongCursor> it = groupSet.iterator();

    while (it.hasNext()) {
      LongCursor cursor = it.next();
      String stringVal = numericToString(ft, cursor.value);
      ft.readableToIndexed(stringVal, term);
      bytesRefs[++index] = term.toBytesRef();
    }

    return new TermInSetQuery(fname, bytesRefs);
  }

  private Query getPointGroupQuery(SchemaField sf,
                                   int size,
                                   LongHashSet groupSet) {

    Iterator<LongCursor> it = groupSet.iterator();
    List<String> values = new ArrayList<>(size);
    FieldType ft = sf.getType();
    while (it.hasNext()) {
      LongCursor cursor = it.next();
      values.add(numericToString(ft, cursor.value));
    }

    return sf.getType().getSetQuery(null, sf, values);
  }

  private static String numericToString(FieldType fieldType, long val) {
    if (fieldType.getNumberType() != null) {
      switch (fieldType.getNumberType()) {
        case INTEGER:
        case LONG:
          return Long.toString(val);
        case FLOAT:
          return Float.toString(Float.intBitsToFloat((int)val));
        case DOUBLE:
          return Double.toString(Double.longBitsToDouble(val));
        case DATE:
          break;
      }
    }
    throw new IllegalArgumentException("FieldType must be INT,LONG,FLOAT,DOUBLE found " + fieldType);
  }

  private Query getGroupQuery(String fname,
                              int size,
                              IntObjectHashMap<BytesRef> ordBytes) {
    BytesRef[] bytesRefs = new BytesRef[size];
    int index = -1;
    Iterator<IntObjectCursor<BytesRef>>it = ordBytes.iterator();
    while (it.hasNext()) {
      IntObjectCursor<BytesRef> cursor = it.next();
      bytesRefs[++index] = cursor.value;
    }
    return new TermInSetQuery(fname, bytesRefs);
  }


  ////////////////////////////////////////////
  ///  SolrInfoBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "Expand Component";
  }

  @Override
  public Category getCategory() {
    return Category.QUERY;
  }

}

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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
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
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.search.CollapsingQParserPlugin;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QParser;
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
 * <p>
 * http parameters:
 * <p>
 * expand=true <br>
 * expand.rows=5 <br>
 * expand.sort=field asc|desc<br>
 * expand.q=*:* (optional, overrides the main query)<br>
 * expand.fq=type:child (optional, overrides the main filter queries)<br>
 * expand.field=field (mandatory if the not used with the CollapsingQParserPlugin)<br>
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
        for (Query q : filters) {
          if (q instanceof CollapsingQParserPlugin.CollapsingPostFilter) {
            CollapsingQParserPlugin.CollapsingPostFilter cp = (CollapsingQParserPlugin.CollapsingPostFilter) q;
            field = cp.getField();
            hint = cp.hint;
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

    Query query;
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
          if (fq != null && fq.trim().length() != 0 && !fq.equals("*:*")) {
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
    long nullValue = 0L;

    if(fieldType instanceof StrField) {
      //Get The Top Level SortedDocValues
      if(CollapsingQParserPlugin.HINT_TOP_FC.equals(hint)) {
        @SuppressWarnings("resource")
        LeafReader uninvertingReader = CollapsingQParserPlugin.getTopFieldCacheReader(searcher, field);
        values = uninvertingReader.getSortedDocValues(field);
      } else {
        values = DocValues.getSorted(reader, field);
      }
    } else if (fieldType.getNumberType() != null) {
      //Get the nullValue for the numeric collapse field
      String defaultValue = searcher.getSchema().getField(field).getDefaultValue();
      
      final NumberType numType = fieldType.getNumberType();

      // Since the expand component depends on the operation of the collapse component, 
      // which validates that numeric field types are 32-bit,
      // we don't need to handle invalid 64-bit field types here.
      // FIXME: what happens when expand.field specified?
      //  how would this work for date field?
      //  SOLR-10400: before this, long and double were explicitly handled
      if (defaultValue != null) {
        if (numType == NumberType.INTEGER) {
          nullValue = Long.parseLong(defaultValue);
        } else if (numType == NumberType.FLOAT) {
          nullValue = Float.floatToIntBits(Float.parseFloat(defaultValue));
        }
      } else if (NumberType.FLOAT.equals(numType)) { // Integer case already handled by nullValue defaulting to 0
        nullValue = Float.floatToIntBits(0.0f);
      }
    } else {
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
      int count = 0;

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

        int contextDoc = globalDoc - currentDocBase;
        if(ordinalMap != null) {
          if (contextDoc > currentValues.docID()) {
            currentValues.advance(contextDoc);
          }
          if (contextDoc == currentValues.docID()) {
            int ord = currentValues.ordValue();
            ++count;
            BytesRef ref = currentValues.lookupOrd(ord);
            ord = (int)segmentOrdinalMap.get(ord);
            ordBytes.put(ord, BytesRef.deepCopyOf(ref));
            groupBits.set(ord);
            collapsedSet.add(globalDoc);
          }
        } else {
          if (globalDoc > values.docID()) {
            values.advance(globalDoc);
          }
          if (globalDoc == values.docID()) {
            int ord = values.ordValue();
            ++count;
            BytesRef ref = values.lookupOrd(ord);
            ordBytes.put(ord, BytesRef.deepCopyOf(ref));
            groupBits.set(ord);
            collapsedSet.add(globalDoc);
          }
        }
      }

      if(count > 0 && count < 200) {
        groupQuery = getGroupQuery(field, count, ordBytes);
      }
    } else {
      groupSet = new LongHashSet(docList.size());
      NumericDocValues collapseValues = contexts.get(currentContext).reader().getNumericDocValues(field);
      int count = 0;
      for(int i=0; i<globalDocs.length; i++) {
        int globalDoc = globalDocs[i];
        while(globalDoc >= nextDocBase) {
          currentContext++;
          currentDocBase = contexts.get(currentContext).docBase;
          nextDocBase = currentContext+1 < contexts.size() ? contexts.get(currentContext+1).docBase : Integer.MAX_VALUE;
          collapseValues = contexts.get(currentContext).reader().getNumericDocValues(field);
        }
        int contextDoc = globalDoc - currentDocBase;
        int valueDocID = collapseValues.docID();
        if (valueDocID < contextDoc) {
          valueDocID = collapseValues.advance(contextDoc);
        }
        long value;
        if (valueDocID == contextDoc) {
          value = collapseValues.longValue();
        } else {
          value = 0;
        }
        if(value != nullValue) {
          ++count;
          groupSet.add(value);
          collapsedSet.add(globalDoc);
        }
      }

      if(count > 0 && count < 200) {
        if (fieldType.isPointField()) {
          groupQuery = getPointGroupQuery(schemaField, count, groupSet);
        } else {
          groupQuery = getGroupQuery(field, fieldType, count, groupSet);
        }
      }
    }

    Collector collector;
    if (sort != null)
      sort = sort.rewrite(searcher);


    Collector groupExpandCollector = null;

    if(values != null) {
      //Get The Top Level SortedDocValues again so we can re-iterate:
      if(CollapsingQParserPlugin.HINT_TOP_FC.equals(hint)) {
        @SuppressWarnings("resource")
        LeafReader uninvertingReader = CollapsingQParserPlugin.getTopFieldCacheReader(searcher, field);
        values = uninvertingReader.getSortedDocValues(field);
      } else {
        values = DocValues.getSorted(reader, field);
      }
      
      groupExpandCollector = new GroupExpandCollector(values, groupBits, collapsedSet, limit, sort);
    } else {
      groupExpandCollector = new NumericGroupExpandCollector(field, nullValue, groupSet, collapsedSet, limit, sort);
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

    if (pfilter.filter != null) {
      query = new BooleanQuery.Builder()
          .add(query, Occur.MUST)
          .add(pfilter.filter, Occur.FILTER)
          .build();
    }
    searcher.search(query, collector);

    ReturnFields returnFields = rb.rsp.getReturnFields();
    LongObjectMap<Collector> groups = ((GroupCollector) groupExpandCollector).getGroups();
    NamedList outMap = new SimpleOrderedMap();
    CharsRefBuilder charsRef = new CharsRefBuilder();
    for (LongObjectCursor<Collector> cursor : groups) {
      long groupValue = cursor.key;
      TopDocsCollector<?> topDocsCollector = TopDocsCollector.class.cast(cursor.value);
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
        DocSlice slice = new DocSlice(0, docs.length, docs, scores, topDocs.totalHits.value, Float.NaN);

        if(fieldType instanceof StrField) {
          final BytesRef bytesRef = ordBytes.get((int)groupValue);
          fieldType.indexedToReadable(bytesRef, charsRef);
          String group = charsRef.toString();
          outMap.add(group, slice);
        } else {
          outMap.add(numericToString(fieldType, groupValue), slice);
        }
      }
    }

    rb.rsp.add("expanded", outMap);
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

  @SuppressWarnings("unchecked")
  @Override
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

  private static class GroupExpandCollector implements Collector, GroupCollector {
    private SortedDocValues docValues;
    private OrdinalMap ordinalMap;
    private SortedDocValues segmentValues;
    private LongValues segmentOrdinalMap;
    private MultiDocValues.MultiSortedDocValues multiSortedDocValues;

    private LongObjectMap<Collector> groups;
    private FixedBitSet groupBits;
    private IntHashSet collapsedSet;

    public GroupExpandCollector(SortedDocValues docValues, FixedBitSet groupBits, IntHashSet collapsedSet, int limit, Sort sort) throws IOException {
      int numGroups = collapsedSet.size();
      groups = new LongObjectHashMap<>(numGroups);
      DocIdSetIterator iterator = new BitSetIterator(groupBits, 0); // cost is not useful here
      int group;
      while ((group = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        Collector collector = (sort == null) ? TopScoreDocCollector.create(limit, Integer.MAX_VALUE) : TopFieldCollector.create(sort, limit, Integer.MAX_VALUE);
        groups.put(group, collector);
      }

      this.collapsedSet = collapsedSet;
      this.groupBits = groupBits;
      this.docValues = docValues;
      if(docValues instanceof MultiDocValues.MultiSortedDocValues) {
        this.multiSortedDocValues = (MultiDocValues.MultiSortedDocValues)docValues;
        this.ordinalMap = multiSortedDocValues.mapping;
      }
    }

    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      final int docBase = context.docBase;

      if(ordinalMap != null) {
        this.segmentValues = this.multiSortedDocValues.values[context.ord];
        this.segmentOrdinalMap = ordinalMap.getGlobalOrds(context.ord);
      }

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
        }

        @Override
        public void collect(int docId) throws IOException {
          int globalDoc = docId + docBase;
          int ord = -1;
          if(ordinalMap != null) {
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

          if (ord > -1 && groupBits.get(ord) && !collapsedSet.contains(globalDoc)) {
            LeafCollector c = leafCollectors.get(ord);
            c.collect(docId);
          }
        }
      };
    }

    public LongObjectMap<Collector> getGroups() {
      return groups;
    }
  }

  private static class NumericGroupExpandCollector implements Collector, GroupCollector {
    private NumericDocValues docValues;

    private String field;
    private LongObjectHashMap<Collector> groups;

    private IntHashSet collapsedSet;
    private long nullValue;

    public NumericGroupExpandCollector(String field, long nullValue, LongHashSet groupSet, IntHashSet collapsedSet, int limit, Sort sort) throws IOException {
      int numGroups = collapsedSet.size();
      this.nullValue = nullValue;
      groups = new LongObjectHashMap<>(numGroups);
      Iterator<LongCursor> iterator = groupSet.iterator();
      while (iterator.hasNext()) {
        LongCursor cursor = iterator.next();
        Collector collector = (sort == null) ? TopScoreDocCollector.create(limit, Integer.MAX_VALUE) : TopFieldCollector.create(sort, limit, Integer.MAX_VALUE);
        groups.put(cursor.value, collector);
      }

      this.field = field;
      this.collapsedSet = collapsedSet;
    }

    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      final int docBase = context.docBase;
      this.docValues = context.reader().getNumericDocValues(this.field);

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
        }

        @Override
        public void collect(int docId) throws IOException {
          long value;
          if (docValues.advanceExact(docId)) {
            value = docValues.longValue();
          } else {
            value = 0;
          }
          final int index;
          if (value != nullValue && 
              (index = leafCollectors.indexOf(value)) >= 0 && 
              !collapsedSet.contains(docId + docBase)) {
            leafCollectors.indexGet(index).collect(docId);
          }
        }
      };
    }

    public LongObjectHashMap<Collector> getGroups() {
      return groups;
    }

  }

  //TODO lets just do simple abstract base class -- a fine use of inheritance
  private interface GroupCollector extends Collector {
    public LongObjectMap<Collector> getGroups();

    @Override
    default ScoreMode scoreMode() {
      final LongObjectMap<Collector> groups = getGroups();
      if (groups.isEmpty()) {
        return ScoreMode.COMPLETE; // doesn't matter?
      } else {
        return groups.iterator().next().value.scoreMode(); // we assume all the collectors should have the same nature
      }
    }
  }

  private Query getGroupQuery(String fname,
                           FieldType ft,
                           int size,
                           LongHashSet groupSet) {

    List<BytesRef> bytesRefs = new ArrayList<>(size);
    BytesRefBuilder term = new BytesRefBuilder();
    Iterator<LongCursor> it = groupSet.iterator();

    while (it.hasNext()) {
      LongCursor cursor = it.next();
      String stringVal = numericToString(ft, cursor.value);
      ft.readableToIndexed(stringVal, term);
      bytesRefs.add(term.toBytesRef());
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

  private String numericToString(FieldType fieldType, long val) {
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
    List<BytesRef> bytesRefs = new ArrayList<>(size);
    Iterator<IntObjectCursor<BytesRef>>it = ordBytes.iterator();
    while (it.hasNext()) {
      IntObjectCursor<BytesRef> cursor = it.next();
      bytesRefs.add(cursor.value);
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

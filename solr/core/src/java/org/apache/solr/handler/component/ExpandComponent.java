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

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ExpandParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.CollapsingQParserPlugin;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The ExpandComponent is designed to work with the CollapsingPostFilter.
 * The CollapsingPostFilter collapses a result set on a field.
 * <p/>
 * The ExpandComponent expands the collapsed groups for a single page.
 * <p/>
 * http parameters:
 * <p/>
 * expand=true <br/>
 * expand.rows=5 <br/>
 * expand.sort=field asc|desc<br/>
 * expand.q=*:* (optional, overrides the main query)<br/>
 * expand.fq=type:child (optional, overrides the main filter queries)<br/>
 * expand.field=field (mandatory if the not used with the CollapsingQParserPlugin)<br/>
 */
public class ExpandComponent extends SearchComponent implements PluginInfoInitialized, SolrCoreAware {
  public static final String COMPONENT_NAME = "expand";
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

    boolean isShard = params.getBool(ShardParams.IS_SHARD, false);
    String ids = params.get(ShardParams.IDS);

    if (ids == null && isShard) {
      return;
    }

    String field = params.get(ExpandParams.EXPAND_FIELD);
    if (field == null) {
      List<Query> filters = rb.getFilters();
      if (filters != null) {
        for (Query q : filters) {
          if (q instanceof CollapsingQParserPlugin.CollapsingPostFilter) {
            CollapsingQParserPlugin.CollapsingPostFilter cp = (CollapsingQParserPlugin.CollapsingPostFilter) q;
            field = cp.getField();
          }
        }
      }
    }

    if (field == null) {
      throw new IOException("Expand field is null.");
    }

    String sortParam = params.get(ExpandParams.EXPAND_SORT);
    String[] fqs = params.getParams(ExpandParams.EXPAND_FQ);
    String qs = params.get(ExpandParams.EXPAND_Q);
    int limit = params.getInt(ExpandParams.EXPAND_ROWS, 5);

    Sort sort = null;

    if (sortParam != null) {
      sort = QueryParsing.parseSortSpec(sortParam, rb.req).getSort();
    }

    Query query;
    if (qs == null) {
      query = rb.getQuery();
    } else {
      try {
        QParser parser = QParser.getParser(qs, null, req);
        query = parser.getQuery();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    List<Query> newFilters = new ArrayList<>();

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
      try {
        for (String fq : fqs) {
          if (fq != null && fq.trim().length() != 0 && !fq.equals("*:*")) {
            QParser fqp = QParser.getParser(fq, null, req);
            newFilters.add(fqp.getQuery());
          }
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    SolrIndexSearcher searcher = req.getSearcher();
    LeafReader reader = searcher.getLeafReader();
    SortedDocValues values = DocValues.getSorted(reader, field);
    FixedBitSet groupBits = new FixedBitSet(values.getValueCount());
    DocList docList = rb.getResults().docList;
    IntOpenHashSet collapsedSet = new IntOpenHashSet(docList.size() * 2);

    DocIterator idit = docList.iterator();

    while (idit.hasNext()) {
      int doc = idit.nextDoc();
      int ord = values.getOrd(doc);
      if (ord > -1) {
        groupBits.set(ord);
        collapsedSet.add(doc);
      }
    }

    Collector collector;
    if (sort != null)
      sort = sort.rewrite(searcher);
    GroupExpandCollector groupExpandCollector = new GroupExpandCollector(values, groupBits, collapsedSet, limit, sort);
    SolrIndexSearcher.ProcessedFilter pfilter = searcher.getProcessedFilter(null, newFilters);
    if (pfilter.postFilter != null) {
      pfilter.postFilter.setLastDelegate(groupExpandCollector);
      collector = pfilter.postFilter;
    } else {
      collector = groupExpandCollector;
    }

    searcher.search(query, pfilter.filter, collector);
    IntObjectMap groups = groupExpandCollector.getGroups();
    Map<String, DocSlice> outMap = new HashMap<>();
    CharsRefBuilder charsRef = new CharsRefBuilder();
    FieldType fieldType = searcher.getSchema().getField(field).getType();
    for (IntObjectCursor cursor : (Iterable<IntObjectCursor>) groups) {
      int ord = cursor.key;
      TopDocsCollector topDocsCollector = (TopDocsCollector) cursor.value;
      TopDocs topDocs = topDocsCollector.topDocs();
      ScoreDoc[] scoreDocs = topDocs.scoreDocs;
      if (scoreDocs.length > 0) {
        int[] docs = new int[scoreDocs.length];
        float[] scores = new float[scoreDocs.length];
        for (int i = 0; i < docs.length; i++) {
          ScoreDoc scoreDoc = scoreDocs[i];
          docs[i] = scoreDoc.doc;
          scores[i] = scoreDoc.score;
        }
        DocSlice slice = new DocSlice(0, docs.length, docs, scores, topDocs.totalHits, topDocs.getMaxScore());
        final BytesRef bytesRef = values.lookupOrd(ord);
        fieldType.indexedToReadable(bytesRef, charsRef);
        String group = charsRef.toString();
        outMap.put(group, slice);
      }
    }

    rb.rsp.add("expanded", outMap);
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {

  }

  @SuppressWarnings("unchecked")
  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {

    if (!rb.doExpand) {
      return;
    }

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
      SolrQueryRequest req = rb.req;
      Map expanded = (Map) req.getContext().get("expanded");
      if (expanded == null) {
        expanded = new HashMap();
        req.getContext().put("expanded", expanded);
      }

      for (ShardResponse srsp : sreq.responses) {
        NamedList response = srsp.getSolrResponse().getResponse();
        Map ex = (Map) response.get("expanded");
        for (Map.Entry<String, SolrDocumentList> entry : (Iterable<Map.Entry<String, SolrDocumentList>>) ex.entrySet()) {
          String name = entry.getKey();
          SolrDocumentList val = entry.getValue();
          expanded.put(name, val);
        }
      }
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {

    if (!rb.doExpand) {
      return;
    }

    if (rb.stage != ResponseBuilder.STAGE_GET_FIELDS) {
      return;
    }

    Map expanded = (Map) rb.req.getContext().get("expanded");
    if (expanded == null) {
      expanded = new HashMap();
    }

    rb.rsp.add("expanded", expanded);
  }

  private class GroupExpandCollector implements Collector {
    private SortedDocValues docValues;
    private IntObjectMap<Collector> groups;
    private int docBase;
    private FixedBitSet groupBits;
    private IntOpenHashSet collapsedSet;

    public GroupExpandCollector(SortedDocValues docValues, FixedBitSet groupBits, IntOpenHashSet collapsedSet, int limit, Sort sort) throws IOException {
      int numGroups = collapsedSet.size();
      groups = new IntObjectOpenHashMap<>(numGroups * 2);
      DocIdSetIterator iterator = groupBits.iterator();
      int group;
      while ((group = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        Collector collector = (sort == null) ? TopScoreDocCollector.create(limit, true) : TopFieldCollector.create(sort, limit, false, false, false, true);
        groups.put(group, collector);
      }

      this.collapsedSet = collapsedSet;
      this.groupBits = groupBits;
      this.docValues = docValues;
    }

    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      final int docBase = context.docBase;
      final IntObjectMap<LeafCollector> leafCollectors = new IntObjectOpenHashMap<>();
      for (IntObjectCursor<Collector> entry : groups) {
        leafCollectors.put(entry.key, entry.value.getLeafCollector(context));
      }
      return new LeafCollector() {

        @Override
        public void setScorer(Scorer scorer) throws IOException {
          for (ObjectCursor<LeafCollector> c : leafCollectors.values()) {
            c.value.setScorer(scorer);
          }
        }

        @Override
        public void collect(int docId) throws IOException {
          int doc = docId + docBase;
          int ord = docValues.getOrd(doc);
          if (ord > -1 && groupBits.get(ord) && !collapsedSet.contains(doc)) {
            LeafCollector c = leafCollectors.get(ord);
            c.collect(docId);
          }
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
          return false;
        }
      };
    }

    public IntObjectMap<Collector> getGroups() {
      return groups;
    }

  }

  ////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "Expand Component";
  }

  @Override
  public URL[] getDocs() {
    try {
      return new URL[]{
          new URL("http://wiki.apache.org/solr/ExpandComponent")
      };
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }
}

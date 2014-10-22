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

import com.carrotsearch.hppc.IntIntOpenHashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.SentinelIntSet;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.QueryElevationParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.grouping.GroupingSpecification;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.Config;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.ElevatedMarkerFactory;
import org.apache.solr.response.transform.ExcludedMarkerFactory;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpec;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.VersionedFile;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * A component to elevate some documents to the top of the result set.
 *
 * @since solr 1.3
 */
public class QueryElevationComponent extends SearchComponent implements SolrCoreAware {
  private static Logger log = LoggerFactory.getLogger(QueryElevationComponent.class);

  // Constants used in solrconfig.xml
  static final String FIELD_TYPE = "queryFieldType";
  static final String CONFIG_FILE = "config-file";
  static final String EXCLUDE = "exclude";
  public static final String BOOSTED = "BOOSTED";
  public static final String BOOSTED_DOCIDS = "BOOSTED_DOCIDS";
  public static final String BOOSTED_PRIORITY = "BOOSTED_PRIORITY";


  public static final String EXCLUDED = "EXCLUDED";

  // Runtime param -- should be in common?

  private SolrParams initArgs = null;
  private Analyzer analyzer = null;
  private String idField = null;
  private FieldType idSchemaFT;

  boolean forceElevation = false;
  // For each IndexReader, keep a query->elevation map
  // When the configuration is loaded from the data directory.
  // The key is null if loaded from the config directory, and
  // is never re-loaded.
  final Map<IndexReader, Map<String, ElevationObj>> elevationCache =
      new WeakHashMap<>();

  class ElevationObj {
    final String text;
    final String analyzed;
    final TermQuery [] exclude;//just keep the term query, b/c we will not always explicitly exclude the item based on markExcludes query time param
    final BooleanQuery include;
    final Map<BytesRef, Integer> priority;
    final Set<String> ids;
    final Set<String> excludeIds;

    ElevationObj(String qstr, List<String> elevate, List<String> exclude) throws IOException {
      this.text = qstr;
      this.analyzed = getAnalyzedQuery(this.text);
      this.ids = new HashSet<>();
      this.excludeIds = new HashSet<>();

      this.include = new BooleanQuery();
      this.include.setBoost(0);
      this.priority = new HashMap<>();
      int max = elevate.size() + 5;
      for (String id : elevate) {
        id = idSchemaFT.readableToIndexed(id);
        ids.add(id);
        TermQuery tq = new TermQuery(new Term(idField, id));
        include.add(tq, BooleanClause.Occur.SHOULD);
        this.priority.put(new BytesRef(id), max--);
      }

      if (exclude == null || exclude.isEmpty()) {
        this.exclude = null;
      } else {
        this.exclude = new TermQuery[exclude.size()];
        for (int i = 0; i < exclude.size(); i++) {
          String id = idSchemaFT.readableToIndexed(exclude.get(i));
          excludeIds.add(id);
          this.exclude[i] = new TermQuery(new Term(idField, id));
        }
      }
    }
  }

  @Override
  public void init(NamedList args) {
    this.initArgs = SolrParams.toSolrParams(args);
  }

  @Override
  public void inform(SolrCore core) {
    IndexSchema schema = core.getLatestSchema();
    String a = initArgs.get(FIELD_TYPE);
    if (a != null) {
      FieldType ft = schema.getFieldTypes().get(a);
      if (ft == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unknown FieldType: '" + a + "' used in QueryElevationComponent");
      }
      analyzer = ft.getQueryAnalyzer();
    }

    SchemaField sf = schema.getUniqueKeyField();
    if( sf == null) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
          "QueryElevationComponent requires the schema to have a uniqueKeyField." );
    }
    idSchemaFT = sf.getType();
    idField = sf.getName();
    //register the EditorialMarkerFactory
    String excludeName = initArgs.get(QueryElevationParams.EXCLUDE_MARKER_FIELD_NAME, "excluded");
    if (excludeName == null || excludeName.equals("") == true){
      excludeName = "excluded";
    }
    ExcludedMarkerFactory excludedMarkerFactory = new ExcludedMarkerFactory();
    core.addTransformerFactory(excludeName, excludedMarkerFactory);
    ElevatedMarkerFactory elevatedMarkerFactory = new ElevatedMarkerFactory();
    String markerName = initArgs.get(QueryElevationParams.EDITORIAL_MARKER_FIELD_NAME, "elevated");
    if (markerName == null || markerName.equals("") == true) {
      markerName = "elevated";
    }
    core.addTransformerFactory(markerName, elevatedMarkerFactory);
    forceElevation = initArgs.getBool(QueryElevationParams.FORCE_ELEVATION, forceElevation);
    try {
      synchronized (elevationCache) {
        elevationCache.clear();
        String f = initArgs.get(CONFIG_FILE);
        if (f == null) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "QueryElevationComponent must specify argument: '" + CONFIG_FILE
                  + "' -- path to elevate.xml");
        }
        boolean exists = false;

        // check if using ZooKeeper
        ZkController zkController = core.getCoreDescriptor().getCoreContainer().getZkController();
        if (zkController != null) {
          // TODO : shouldn't have to keep reading the config name when it has been read before
          exists = zkController.configFileExists(zkController.getZkStateReader().readConfigName(core.getCoreDescriptor().getCloudDescriptor().getCollectionName()), f);
        } else {
          File fC = new File(core.getResourceLoader().getConfigDir(), f);
          File fD = new File(core.getDataDir(), f);
          if (fC.exists() == fD.exists()) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "QueryElevationComponent missing config file: '" + f + "\n"
                    + "either: " + fC.getAbsolutePath() + " or " + fD.getAbsolutePath() + " must exist, but not both.");
          }
          if (fC.exists()) {
            exists = true;
            log.info("Loading QueryElevation from: " + fC.getAbsolutePath());
            Config cfg = new Config(core.getResourceLoader(), f);
            elevationCache.put(null, loadElevationMap(cfg));
          }
        }
        //in other words, we think this is in the data dir, not the conf dir
        if (!exists) {
          // preload the first data
          RefCounted<SolrIndexSearcher> searchHolder = null;
          try {
            searchHolder = core.getNewestSearcher(false);
            IndexReader reader = searchHolder.get().getIndexReader();
            getElevationMap(reader, core);
          } finally {
            if (searchHolder != null) searchHolder.decref();
          }
        }
      }
    } catch (Exception ex) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error initializing QueryElevationComponent.", ex);
    }
  }

  //get the elevation map from the data dir
  Map<String, ElevationObj> getElevationMap(IndexReader reader, SolrCore core) throws Exception {
    synchronized (elevationCache) {
      Map<String, ElevationObj> map = elevationCache.get(null);
      if (map != null) return map;

      map = elevationCache.get(reader);
      if (map == null) {
        String f = initArgs.get(CONFIG_FILE);
        if (f == null) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "QueryElevationComponent must specify argument: " + CONFIG_FILE);
        }
        log.info("Loading QueryElevation from data dir: " + f);
        
        Config cfg;
        
        ZkController zkController = core.getCoreDescriptor().getCoreContainer().getZkController();
        if (zkController != null) {
          cfg = new Config(core.getResourceLoader(), f, null, null);
        } else {
          InputStream is = VersionedFile.getLatestFile(core.getDataDir(), f);
          cfg = new Config(core.getResourceLoader(), f, new InputSource(is), null);
        }
  
        map = loadElevationMap(cfg);
        elevationCache.put(reader, map);
      }
      return map;
    }
  }

  //load up the elevation map
  private Map<String, ElevationObj> loadElevationMap(Config cfg) throws IOException {
    XPath xpath = XPathFactory.newInstance().newXPath();
    Map<String, ElevationObj> map = new HashMap<>();
    NodeList nodes = (NodeList) cfg.evaluate("elevate/query", XPathConstants.NODESET);
    for (int i = 0; i < nodes.getLength(); i++) {
      Node node = nodes.item(i);
      String qstr = DOMUtil.getAttr(node, "text", "missing query 'text'");

      NodeList children = null;
      try {
        children = (NodeList) xpath.evaluate("doc", node, XPathConstants.NODESET);
      } catch (XPathExpressionException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "query requires '<doc .../>' child");
      }

      ArrayList<String> include = new ArrayList<>();
      ArrayList<String> exclude = new ArrayList<>();
      for (int j = 0; j < children.getLength(); j++) {
        Node child = children.item(j);
        String id = DOMUtil.getAttr(child, "id", "missing 'id'");
        String e = DOMUtil.getAttr(child, EXCLUDE, null);
        if (e != null) {
          if (Boolean.valueOf(e)) {
            exclude.add(id);
            continue;
          }
        }
        include.add(id);
      }

      ElevationObj elev = new ElevationObj(qstr, include, exclude);
      if (map.containsKey(elev.analyzed)) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Boosting query defined twice for query: '" + elev.text + "' (" + elev.analyzed + "')");
      }
      map.put(elev.analyzed, elev);
    }
    return map;
  }

  /**
   * Helpful for testing without loading config.xml
   *
   * @throws IOException If there is a low-level I/O error.
   */
  void setTopQueryResults(IndexReader reader, String query, String[] ids, String[] ex) throws IOException {
    if (ids == null) {
      ids = new String[0];
    }
    if (ex == null) {
      ex = new String[0];
    }

    Map<String, ElevationObj> elev = elevationCache.get(reader);
    if (elev == null) {
      elev = new HashMap<>();
      elevationCache.put(reader, elev);
    }
    ElevationObj obj = new ElevationObj(query, Arrays.asList(ids), Arrays.asList(ex));
    elev.put(obj.analyzed, obj);
  }

  String getAnalyzedQuery(String query) throws IOException {
    if (analyzer == null) {
      return query;
    }
    StringBuilder norm = new StringBuilder();
    try (TokenStream tokens = analyzer.tokenStream("", query)) {
      tokens.reset();

      CharTermAttribute termAtt = tokens.addAttribute(CharTermAttribute.class);
      while (tokens.incrementToken()) {
        norm.append(termAtt.buffer(), 0, termAtt.length());
      }
      tokens.end();
      return norm.toString();
    }
  }

  //---------------------------------------------------------------------------------
  // SearchComponent
  //---------------------------------------------------------------------------------

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams();
    // A runtime param can skip 
    if (!params.getBool(QueryElevationParams.ENABLE, true)) {
      return;
    }

    boolean exclusive = params.getBool(QueryElevationParams.EXCLUSIVE, false);
    // A runtime parameter can alter the config value for forceElevation
    boolean force = params.getBool(QueryElevationParams.FORCE_ELEVATION, forceElevation);
    boolean markExcludes = params.getBool(QueryElevationParams.MARK_EXCLUDES, false);
    String boostStr = params.get(QueryElevationParams.IDS);
    String exStr = params.get(QueryElevationParams.EXCLUDE);

    Query query = rb.getQuery();
    SolrParams localParams = rb.getQparser().getLocalParams();
    String qstr = localParams == null ? rb.getQueryString() : localParams.get(QueryParsing.V);
    if (query == null || qstr == null) {
      return;
    }

    ElevationObj booster = null;
    try {
      if(boostStr != null || exStr != null) {
        List<String> boosts = (boostStr != null) ? StrUtils.splitSmart(boostStr,",", true) : new ArrayList<String>(0);
        List<String> excludes = (exStr != null) ? StrUtils.splitSmart(exStr, ",", true) : new ArrayList<String>(0);
        booster = new ElevationObj(qstr, boosts, excludes);
      } else {
        IndexReader reader = req.getSearcher().getIndexReader();
        qstr = getAnalyzedQuery(qstr);
        booster = getElevationMap(reader, req.getCore()).get(qstr);
      }
    } catch (Exception ex) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error loading elevation", ex);
    }

    if (booster != null) {
      rb.req.getContext().put(BOOSTED, booster.ids);
      rb.req.getContext().put(BOOSTED_PRIORITY, booster.priority);

      // Change the query to insert forced documents
      if (exclusive == true) {
        //we only want these results
        rb.setQuery(booster.include);
      } else {
        BooleanQuery newq = new BooleanQuery(true);
        newq.add(query, BooleanClause.Occur.SHOULD);
        newq.add(booster.include, BooleanClause.Occur.SHOULD);
        if (booster.exclude != null) {
          if (markExcludes == false) {
            for (TermQuery tq : booster.exclude) {
              newq.add(new BooleanClause(tq, BooleanClause.Occur.MUST_NOT));
            }
          } else {
            //we are only going to mark items as excluded, not actually exclude them.  This works
            //with the EditorialMarkerFactory
            rb.req.getContext().put(EXCLUDED, booster.excludeIds);
          }
        }
        rb.setQuery(newq);
      }

      ElevationComparatorSource comparator = new ElevationComparatorSource(booster);
      // if the sort is 'score desc' use a custom sorting method to 
      // insert documents in their proper place 
      SortSpec sortSpec = rb.getSortSpec();
      if (sortSpec.getSort() == null) {
        sortSpec.setSortAndFields(new Sort(new SortField[]{
              new SortField("_elevate_", comparator, true),
              new SortField(null, SortField.Type.SCORE, false)
            }),
          Arrays.asList(new SchemaField[2]));
      } else {
        // Check if the sort is based on score
        SortSpec modSortSpec = this.modifySortSpec(sortSpec, force, comparator);
        if (null != modSortSpec) {
          rb.setSortSpec(modSortSpec);
        }
      }

      // alter the sorting in the grouping specification if there is one
      GroupingSpecification groupingSpec = rb.getGroupingSpec();
      if(groupingSpec != null) {
        SortField[] groupSort = groupingSpec.getGroupSort().getSort();
        Sort modGroupSort = this.modifySort(groupSort, force, comparator);
        if(modGroupSort != null) {
          groupingSpec.setGroupSort(modGroupSort);
        }
        SortField[] withinGroupSort = groupingSpec.getSortWithinGroup().getSort();
        Sort modWithinGroupSort = this.modifySort(withinGroupSort, force, comparator);
        if(modWithinGroupSort != null) {
          groupingSpec.setSortWithinGroup(modWithinGroupSort);
        }
      }
    }

    // Add debugging information
    if (rb.isDebug()) {
      List<String> match = null;
      if (booster != null) {
        // Extract the elevated terms into a list
        match = new ArrayList<>(booster.priority.size());
        for (Object o : booster.include.clauses()) {
          TermQuery tq = (TermQuery) ((BooleanClause) o).getQuery();
          match.add(tq.getTerm().text());
        }
      }

      SimpleOrderedMap<Object> dbg = new SimpleOrderedMap<>();
      dbg.add("q", qstr);
      dbg.add("match", match);
      if (rb.isDebugQuery()) {
        rb.addDebugInfo("queryBoosting", dbg);
      }
    }
  }

  private Sort modifySort(SortField[] current, boolean force, ElevationComparatorSource comparator) {
    SortSpec tmp = new SortSpec(new Sort(current), Arrays.asList(new SchemaField[current.length]));
    tmp = modifySortSpec(tmp, force, comparator);
    return null == tmp ? null : tmp.getSort();
  }

  private SortSpec modifySortSpec(SortSpec current, boolean force, ElevationComparatorSource comparator) {
    boolean modify = false;
    SortField[] currentSorts = current.getSort().getSort();
    List<SchemaField> currentFields = current.getSchemaFields();

    ArrayList<SortField> sorts = new ArrayList<>(currentSorts.length + 1);
    List<SchemaField> fields = new ArrayList<>(currentFields.size() + 1);

    // Perhaps force it to always sort by score
    if (force && currentSorts[0].getType() != SortField.Type.SCORE) {
      sorts.add(new SortField("_elevate_", comparator, true));
      fields.add(null);
      modify = true;
    }
    for (int i = 0; i < currentSorts.length; i++) {
      SortField sf = currentSorts[i];
      if (sf.getType() == SortField.Type.SCORE) {
        sorts.add(new SortField("_elevate_", comparator, !sf.getReverse()));
        fields.add(null);
        modify = true;
      }
      sorts.add(sf);
      fields.add(currentFields.get(i));
    }
    if (modify) {
      SortSpec newSpec = new SortSpec(new Sort(sorts.toArray(new SortField[sorts.size()])),
                                      fields);
      newSpec.setOffset(current.getOffset());
      newSpec.setCount(current.getCount());
      return newSpec;
    }
    return null;
  }


  public static IntIntOpenHashMap getBoostDocs(SolrIndexSearcher indexSearcher, Map<BytesRef, Integer>boosted, Map context) throws IOException {

    IntIntOpenHashMap boostDocs = null;

    if(boosted != null) {

      //First see if it's already in the request context. Could have been put there
      //by another caller.
      if(context != null) {
        boostDocs = (IntIntOpenHashMap)context.get(BOOSTED_DOCIDS);
      }

      if(boostDocs != null) {
        return boostDocs;
      }
      //Not in the context yet so load it.

      SchemaField idField = indexSearcher.getSchema().getUniqueKeyField();
      String fieldName = idField.getName();
      HashSet<BytesRef> localBoosts = new HashSet(boosted.size()*2);
      Iterator<BytesRef> boostedIt = boosted.keySet().iterator();
      while(boostedIt.hasNext()) {
        localBoosts.add(boostedIt.next());
      }

      boostDocs = new IntIntOpenHashMap(boosted.size()*2);

      List<LeafReaderContext>leaves = indexSearcher.getTopReaderContext().leaves();
      TermsEnum termsEnum = null;
      DocsEnum docsEnum = null;
      for(LeafReaderContext leaf : leaves) {
        LeafReader reader = leaf.reader();
        int docBase = leaf.docBase;
        Bits liveDocs = reader.getLiveDocs();
        Terms terms = reader.terms(fieldName);
        termsEnum = terms.iterator(termsEnum);
        Iterator<BytesRef> it = localBoosts.iterator();
        while(it.hasNext()) {
          BytesRef ref = it.next();
          if(termsEnum.seekExact(ref)) {
            docsEnum = termsEnum.docs(liveDocs, docsEnum);
            int doc = docsEnum.nextDoc();
            if(doc != DocsEnum.NO_MORE_DOCS) {
              //Found the document.
              int p = boosted.get(ref);
              boostDocs.put(doc+docBase, p);
              it.remove();
            }
          }
        }
      }
    }

    if(context != null) {
      context.put(BOOSTED_DOCIDS, boostDocs);
    }

    return boostDocs;
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    // Do nothing -- the real work is modifying the input query
  }

  //---------------------------------------------------------------------------------
  // SolrInfoMBean
  //---------------------------------------------------------------------------------

  @Override
  public String getDescription() {
    return "Query Boosting -- boost particular documents for a given query";
  }

  @Override
  public URL[] getDocs() {
    try {
      return new URL[]{
          new URL("http://wiki.apache.org/solr/QueryElevationComponent")
      };
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }
  class ElevationComparatorSource extends FieldComparatorSource {
  private QueryElevationComponent.ElevationObj elevations;
  private SentinelIntSet ordSet; //the key half of the map
  private BytesRef[] termValues;//the value half of the map

  public ElevationComparatorSource(final QueryElevationComponent.ElevationObj elevations) {
    this.elevations = elevations;
    int size = elevations.ids.size();
    ordSet = new SentinelIntSet(size, -1);
    termValues = new BytesRef[ordSet.keys.length];
  }

  @Override
  public FieldComparator<Integer> newComparator(String fieldname, final int numHits, int sortPos, boolean reversed) throws IOException {
    return new FieldComparator<Integer>() {
      private final int[] values = new int[numHits];
      private int bottomVal;
      private int topVal;
      private TermsEnum termsEnum;
      private DocsEnum docsEnum;
      Set<String> seen = new HashSet<>(elevations.ids.size());

      @Override
      public int compare(int slot1, int slot2) {
        return values[slot1] - values[slot2];  // values will be small enough that there is no overflow concern
      }

      @Override
      public void setBottom(int slot) {
        bottomVal = values[slot];
      }

      @Override
      public void setTopValue(Integer value) {
        topVal = value.intValue();
      }

      private int docVal(int doc) {
        if (ordSet.size() > 0) {
          int slot = ordSet.find(doc);
          if (slot >= 0) {
            BytesRef id = termValues[slot];
            Integer prio = elevations.priority.get(id);
            return prio == null ? 0 : prio.intValue();
          }
        }
        return 0;
      }

      @Override
      public int compareBottom(int doc) {
        return bottomVal - docVal(doc);
      }

      @Override
      public void copy(int slot, int doc) {
        values[slot] = docVal(doc);
      }

      @Override
      public FieldComparator setNextReader(LeafReaderContext context) throws IOException {
        //convert the ids to Lucene doc ids, the ordSet and termValues needs to be the same size as the number of elevation docs we have
        ordSet.clear();
        Fields fields = context.reader().fields();
        if (fields == null) return this;
        Terms terms = fields.terms(idField);
        if (terms == null) return this;
        termsEnum = terms.iterator(termsEnum);
        BytesRefBuilder term = new BytesRefBuilder();
        Bits liveDocs = context.reader().getLiveDocs();

        for (String id : elevations.ids) {
          term.copyChars(id);
          if (seen.contains(id) == false  && termsEnum.seekExact(term.get())) {
            docsEnum = termsEnum.docs(liveDocs, docsEnum, DocsEnum.FLAG_NONE);
            if (docsEnum != null) {
              int docId = docsEnum.nextDoc();
              if (docId == DocIdSetIterator.NO_MORE_DOCS ) continue;  // must have been deleted
              termValues[ordSet.put(docId)] = term.toBytesRef();
              seen.add(id);
              assert docsEnum.nextDoc() == DocIdSetIterator.NO_MORE_DOCS;
            }
          }
        }
        return this;
      }

      @Override
      public Integer value(int slot) {
        return values[slot];
      }

      @Override
      public int compareTop(int doc) {
        final int docValue = docVal(doc);
        return topVal - docValue;  // values will be small enough that there is no overflow concern
      }
    };
  }
  }
}

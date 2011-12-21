/**
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SentinelIntSet;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.QueryElevationParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.Config;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.EditorialMarkerFactory;
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
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

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
      new WeakHashMap<IndexReader, Map<String, ElevationObj>>();

  class ElevationObj {
    final String text;
    final String analyzed;
    final BooleanClause[] exclude;
    final BooleanQuery include;
    final Map<BytesRef, Integer> priority;
    final Set<String> ids;

    ElevationObj(String qstr, List<String> elevate, List<String> exclude) throws IOException {
      this.text = qstr;
      this.analyzed = getAnalyzedQuery(this.text);
      this.ids = new HashSet<String>();

      this.include = new BooleanQuery();
      this.include.setBoost(0);
      this.priority = new HashMap<BytesRef, Integer>();
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
        this.exclude = new BooleanClause[exclude.size()];
        for (int i = 0; i < exclude.size(); i++) {
          TermQuery tq = new TermQuery(new Term(idField, idSchemaFT.readableToIndexed(exclude.get(i))));
          this.exclude[i] = new BooleanClause(tq, BooleanClause.Occur.MUST_NOT);
        }
      }
    }
  }

  @Override
  public void init(NamedList args) {
    this.initArgs = SolrParams.toSolrParams(args);
  }

  public void inform(SolrCore core) {
    String a = initArgs.get(FIELD_TYPE);
    if (a != null) {
      FieldType ft = core.getSchema().getFieldTypes().get(a);
      if (ft == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unknown FieldType: '" + a + "' used in QueryElevationComponent");
      }
      analyzer = ft.getQueryAnalyzer();
    }

    SchemaField sf = core.getSchema().getUniqueKeyField();
    if( sf == null) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
          "QueryElevationComponent requires the schema to have a uniqueKeyField." );
    }
    idSchemaFT = sf.getType();
    idField = sf.getName();
    //register the EditorialMarkerFactory
    EditorialMarkerFactory factory = new EditorialMarkerFactory();
    String markerName = initArgs.get(QueryElevationParams.EDITORIAL_MARKER_FIELD_NAME, "elevated");
    if (markerName == null || markerName.equals("") == true) {
      markerName = "elevated";
    }
    core.addTransformerFactory(markerName, factory);
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
          exists = zkController.configFileExists(zkController.readConfigName(core.getCoreDescriptor().getCloudDescriptor().getCollectionName()), f);
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
          "Error initializing QueryElevationComponent.", ex, false);
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

        InputStream is = VersionedFile.getLatestFile(core.getDataDir(), f);
        Config cfg = new Config(core.getResourceLoader(), f, new InputSource(is), null);
        map = loadElevationMap(cfg);
        elevationCache.put(reader, map);
      }
      return map;
    }
  }

  //load up the elevation map
  private Map<String, ElevationObj> loadElevationMap(Config cfg) throws IOException {
    XPath xpath = XPathFactory.newInstance().newXPath();
    Map<String, ElevationObj> map = new HashMap<String, ElevationObj>();
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

      ArrayList<String> include = new ArrayList<String>();
      ArrayList<String> exclude = new ArrayList<String>();
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
   * @throws IOException
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
      elev = new HashMap<String, ElevationObj>();
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
    TokenStream tokens = analyzer.tokenStream("", new StringReader(query));
    tokens.reset();

    CharTermAttribute termAtt = tokens.addAttribute(CharTermAttribute.class);
    while (tokens.incrementToken()) {
      norm.append(termAtt.buffer(), 0, termAtt.length());
    }
    tokens.end();
    tokens.close();
    return norm.toString();
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

    Query query = rb.getQuery();
    String qstr = rb.getQueryString();
    if (query == null || qstr == null) {
      return;
    }

    qstr = getAnalyzedQuery(qstr);
    IndexReader reader = req.getSearcher().getIndexReader();
    ElevationObj booster = null;
    try {
      booster = getElevationMap(reader, req.getCore()).get(qstr);
    } catch (Exception ex) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error loading elevation", ex);
    }

    if (booster != null) {
      rb.req.getContext().put("BOOSTED", booster.ids);

      // Change the query to insert forced documents
      if (exclusive == true) {
        //we only want these results
        rb.setQuery(booster.include);
      } else {
        BooleanQuery newq = new BooleanQuery(true);
        newq.add(query, BooleanClause.Occur.SHOULD);
        newq.add(booster.include, BooleanClause.Occur.SHOULD);
        if (booster.exclude != null) {
          for (BooleanClause bq : booster.exclude) {
            newq.add(bq);
          }
        }
        rb.setQuery(newq);
      }

      ElevationComparatorSource comparator = new ElevationComparatorSource(booster);
      // if the sort is 'score desc' use a custom sorting method to 
      // insert documents in their proper place 
      SortSpec sortSpec = rb.getSortSpec();
      if (sortSpec.getSort() == null) {
        sortSpec.setSort(new Sort(new SortField[]{
            new SortField(idField, comparator, false),
            new SortField(null, SortField.Type.SCORE, false)
        }));
      } else {
        // Check if the sort is based on score
        boolean modify = false;
        SortField[] current = sortSpec.getSort().getSort();
        ArrayList<SortField> sorts = new ArrayList<SortField>(current.length + 1);
        // Perhaps force it to always sort by score
        if (force && current[0].getType() != SortField.Type.SCORE) {
          sorts.add(new SortField(idField, comparator, false));
          modify = true;
        }
        for (SortField sf : current) {
          if (sf.getType() == SortField.Type.SCORE) {
            sorts.add(new SortField(idField, comparator, sf.getReverse()));
            modify = true;
          }
          sorts.add(sf);
        }
        if (modify) {
          sortSpec.setSort(new Sort(sorts.toArray(new SortField[sorts.size()])));
        }
      }
    }

    // Add debugging information
    if (rb.isDebug()) {
      List<String> match = null;
      if (booster != null) {
        // Extract the elevated terms into a list
        match = new ArrayList<String>(booster.priority.size());
        for (Object o : booster.include.clauses()) {
          TermQuery tq = (TermQuery) ((BooleanClause) o).getQuery();
          match.add(tq.getTerm().text());
        }
      }

      SimpleOrderedMap<Object> dbg = new SimpleOrderedMap<Object>();
      dbg.add("q", qstr);
      dbg.add("match", match);
      if (rb.isDebugQuery()) {
        rb.addDebugInfo("queryBoosting", dbg);
      }
    }
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
  public String getVersion() {
    return "$Revision$";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
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

  public ElevationComparatorSource(final QueryElevationComponent.ElevationObj elevations) throws IOException {
    this.elevations = elevations;
    int size = elevations.ids.size();
    ordSet = new SentinelIntSet(size, -1);
    termValues = new BytesRef[ordSet.keys.length];
  }

  @Override
  public FieldComparator<Integer> newComparator(final String fieldname, final int numHits, int sortPos, boolean reversed) throws IOException {
    return new FieldComparator<Integer>() {
      private final int[] values = new int[numHits];
      private int bottomVal;
      private TermsEnum termsEnum;
      private DocsEnum docsEnum;
      Set<String> seen = new HashSet<String>(elevations.ids.size());

      @Override
      public int compare(int slot1, int slot2) {
        return values[slot2] - values[slot1];  // values will be small enough that there is no overflow concern
      }

      @Override
      public void setBottom(int slot) {
        bottomVal = values[slot];
      }

      private int docVal(int doc) throws IOException {
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
      public int compareBottom(int doc) throws IOException {
        return docVal(doc) - bottomVal;
      }

      @Override
      public void copy(int slot, int doc) throws IOException {
        values[slot] = docVal(doc);
      }

      @Override
      public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
        //convert the ids to Lucene doc ids, the ordSet and termValues needs to be the same size as the number of elevation docs we have
        ordSet.clear();
        Fields fields = context.reader.fields();
        if (fields == null) return this;
        Terms terms = fields.terms(fieldname);
        if (terms == null) return this;
        termsEnum = terms.iterator(termsEnum);
        BytesRef term = new BytesRef();
        Bits liveDocs = context.reader.getLiveDocs();

        for (String id : elevations.ids) {
          term.copyChars(id);
          if (seen.contains(id) == false  && termsEnum.seekExact(term, false)) {
            docsEnum = termsEnum.docs(liveDocs, docsEnum, false);
            if (docsEnum != null) {
              int docId = docsEnum.nextDoc();
              if (docId == DocIdSetIterator.NO_MORE_DOCS ) continue;  // must have been deleted
              termValues[ordSet.put(docId)] = BytesRef.deepCopyOf(term);
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
    };
  }
}
}



package org.apache.solr.search.facet;

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


import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.FunctionQParserPlugin;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;


public abstract class FacetRequest {
  protected Map<String,AggValueSource> facetStats;  // per-bucket statistics
  protected Map<String,FacetRequest> subFacets;     // list of facets
  protected List<String> excludeFilters;
  protected boolean processEmpty;

  public FacetRequest() {
    facetStats = new LinkedHashMap<>();
    subFacets = new LinkedHashMap<>();
  }

  public Map<String, AggValueSource> getFacetStats() {
    return facetStats;
  }

  public Map<String, FacetRequest> getSubFacets() {
    return subFacets;
  }

  public void addStat(String key, AggValueSource stat) {
    facetStats.put(key, stat);
  }

  public void addSubFacet(String key, FacetRequest facetRequest) {
    subFacets.put(key, facetRequest);
  }

  public abstract FacetProcessor createFacetProcessor(FacetContext fcontext);

  public abstract FacetMerger createFacetMerger(Object prototype);
}


class FacetContext {
  // Context info for actually executing a local facet command
  public static final int IS_SHARD=0x01;

  QueryContext qcontext;
  SolrQueryRequest req;  // TODO: replace with params?
  SolrIndexSearcher searcher;
  DocSet base;
  FacetContext parent;
  int flags;

  public boolean isShard() {
    return (flags & IS_SHARD) != 0;
  }

  public FacetContext sub() {
    FacetContext ctx = new FacetContext();
    ctx.flags = flags;
    ctx.qcontext = qcontext;
    ctx.req = req;
    ctx.searcher = searcher;
    ctx.base = base;

    ctx.parent = this;
    return ctx;
  }
}


class FacetProcessor<FacetRequestT extends FacetRequest>  {
  protected SimpleOrderedMap<Object> response;
  protected FacetContext fcontext;
  protected FacetRequestT freq;

  LinkedHashMap<String,SlotAcc> accMap;
  protected SlotAcc[] accs;
  protected CountSlotAcc countAcc;

  FacetProcessor(FacetContext fcontext, FacetRequestT freq) {
    this.fcontext = fcontext;
    this.freq = freq;
  }

  public void process() throws IOException {


  }

  public Object getResponse() {
    return null;
  }


  protected void createAccs(int docCount, int slotCount) throws IOException {
    accMap = new LinkedHashMap<String,SlotAcc>();
    countAcc = new CountSlotAcc(fcontext, slotCount);
    countAcc.key = "count";
    for (Map.Entry<String,AggValueSource> entry : freq.getFacetStats().entrySet()) {
      SlotAcc acc = entry.getValue().createSlotAcc(fcontext, docCount, slotCount);
      acc.key = entry.getKey();
      accMap.put(acc.key, acc);
    }
  }

  /** Create the actual accs array from accMap before starting to collect stats. */
  protected void prepareForCollection() {
    accs = new SlotAcc[accMap.size()];
    int i=0;
    for (SlotAcc acc : accMap.values()) {
      accs[i++] = acc;
    }
  }

  protected void resetStats() {
    countAcc.reset();
    for (SlotAcc acc : accs) {
      acc.reset();
    }
  }

  protected void processStats(SimpleOrderedMap<Object> bucket, DocSet docs, int docCount) throws IOException {
    if (docCount == 0 && !freq.processEmpty || freq.getFacetStats().size() == 0) {
      bucket.add("count", docCount);
      return;
    }
    createAccs(docCount, 1);
    prepareForCollection();
    int collected = collect(docs, 0);
    countAcc.incrementCount(0, collected);
    assert collected == docCount;
    addStats(bucket, 0);
  }


  protected void fillBucketSubs(SimpleOrderedMap<Object> response, FacetContext subContext) throws IOException {
    for (Map.Entry<String,FacetRequest> sub : freq.getSubFacets().entrySet()) {
      FacetProcessor subProcessor = sub.getValue().createFacetProcessor(subContext);
      subProcessor.process();
      response.add( sub.getKey(), subProcessor.getResponse() );
    }
  }

  int collect(DocSet docs, int slot) throws IOException {
    int count = 0;
    SolrIndexSearcher searcher = fcontext.searcher;

    final List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    final Iterator<LeafReaderContext> ctxIt = leaves.iterator();
    LeafReaderContext ctx = null;
    int segBase = 0;
    int segMax;
    int adjustedMax = 0;
    for (DocIterator docsIt = docs.iterator(); docsIt.hasNext(); ) {
      final int doc = docsIt.nextDoc();
      if (doc >= adjustedMax) {
        do {
          ctx = ctxIt.next();
          if (ctx == null) {
            // should be impossible
            throw new RuntimeException("INTERNAL FACET ERROR");
          }
          segBase = ctx.docBase;
          segMax = ctx.reader().maxDoc();
          adjustedMax = segBase + segMax;
        } while (doc >= adjustedMax);
        assert doc >= ctx.docBase;
        setNextReader(ctx);
      }
      count++;
      collect(doc - segBase, slot);  // per-seg collectors
    }
    return count;
  }

  void collect(int segDoc, int slot) throws IOException {
    for (SlotAcc acc : accs) {
      acc.collect(segDoc, slot);
    }
  }

  void setNextReader(LeafReaderContext ctx) throws IOException {
    // countAcc.setNextReader is a no-op
    for (SlotAcc acc : accs) {
      acc.setNextReader(ctx);
    }
  }

  void addStats(SimpleOrderedMap<Object> target, int slotNum) throws IOException {
    int count = countAcc.getCount(slotNum);
    target.add("count", count);
    if (count > 0 || freq.processEmpty) {
      for (SlotAcc acc : accs) {
        acc.setValues(target, slotNum);
      }
    }
  }





  public void fillBucket(SimpleOrderedMap<Object> bucket, Query q) throws IOException {
    boolean needDocSet = freq.getFacetStats().size() > 0 || freq.getSubFacets().size() > 0;

    // TODO: always collect counts or not???

    DocSet result = null;
    int count;

    if (needDocSet) {
      if (q == null) {
        result = fcontext.base;
        // result.incref(); // OFF-HEAP
      } else {
        result = fcontext.searcher.getDocSet(q, fcontext.base);
      }
      count = result.size();
    } else {
      if (q == null) {
        count = fcontext.base.size();
      } else {
        count = fcontext.searcher.numDocs(q, fcontext.base);
      }
    }

    try {
      processStats(bucket, result, (int) count);
      processSubs(bucket, result);
    } finally {
      if (result != null) {
        // result.decref(); // OFF-HEAP
        result = null;
      }
    }
  }




  protected void processSubs(SimpleOrderedMap<Object> bucket, DocSet result) throws IOException {
    // TODO: process exclusions, etc

    if (result == null || result.size() == 0 && !freq.processEmpty) {
      return;
    }

    FacetContext subContext = fcontext.sub();
    subContext.base = result;

    fillBucketSubs(bucket, subContext);
  }


  public static DocSet getFieldMissing(SolrIndexSearcher searcher, DocSet docs, String fieldName) throws IOException {
    SchemaField sf = searcher.getSchema().getField(fieldName);
    DocSet hasVal = searcher.getDocSet(sf.getType().getRangeQuery(null, sf, null, null, false, false));
    DocSet answer = docs.andNot(hasVal);
    // hasVal.decref(); // OFF-HEAP
    return answer;
  }

}





abstract class FacetParser<FacetRequestT extends FacetRequest> {
  protected FacetRequestT facet;
  protected FacetParser parent;
  protected String key;

  public FacetParser(FacetParser parent,String key) {
    this.parent = parent;
    this.key = key;
  }

  public String getKey() {
    return key;
  }

  public String getPathStr() {
    if (parent == null) {
      return "/" + key;
    }
    return parent.getKey() + "/" + key;
  }

  protected RuntimeException err(String msg) {
    return new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg + " ,path="+getPathStr());
  }

  public abstract FacetRequest parse(Object o) throws SyntaxError;

  // TODO: put the FacetRequest on the parser object?
  public void parseSubs(Object o) throws SyntaxError {
    if (o==null) return;
    if (o instanceof Map) {
      Map<String,Object> m = (Map<String, Object>) o;
      for (Map.Entry<String,Object> entry : m.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();

        if ("processEmpty".equals(key)) {
          facet.processEmpty = getBoolean(m, "processEmpty", false);
          continue;
        }

        // "my_prices" : { "range" : { "field":...
        // key="my_prices", value={"range":..

        Object parsedValue = parseFacetOrStat(key, value);

        // TODO: have parseFacetOrStat directly add instead of return?
        if (parsedValue instanceof FacetRequest) {
          facet.addSubFacet(key, (FacetRequest)parsedValue);
        } else if (parsedValue instanceof AggValueSource) {
          facet.addStat(key, (AggValueSource)parsedValue);
        } else {
          throw new RuntimeException("Huh? TODO: " + parsedValue);
        }
      }
    } else {
      // facet : my_field?
      throw err("Expected map for facet/stat");
    }
  }

  public Object parseFacetOrStat(String key, Object o) throws SyntaxError {
   if (o instanceof String) {
     return parseStringFacetOrStat(key, (String)o);
   }

   if (!(o instanceof Map)) {
     throw err("expected Map but got " + o);
   }

   // { "range" : { "field":...
  Map<String,Object> m = (Map<String,Object>)o;
  if (m.size() != 1) {
    throw err("expected facet/stat type name, like {range:{... but got " + m);
  }

    // Is this most efficient way?
    Map.Entry<String,Object> entry = m.entrySet().iterator().next();
    String type = entry.getKey();
    Object args = entry.getValue();
    return parseFacetOrStat(key, type, args);
  }

  public Object parseFacetOrStat(String key, String type, Object args) throws SyntaxError {
    // TODO: a place to register all these facet types?

    if ("field".equals(type) || "terms".equals(type)) {
      return parseFieldFacet(key, args);
    } else if ("query".equals(type)) {
      return parseQueryFacet(key, args);
    } else if ("range".equals(type)) {
     return parseRangeFacet(key, args);
    }

    return parseStat(key, type, args);
  }



  FacetField parseFieldFacet(String key, Object args) throws SyntaxError {
    FacetFieldParser parser = new FacetFieldParser(this, key);
    return parser.parse(args);
  }

  FacetQuery parseQueryFacet(String key, Object args) throws SyntaxError {
    FacetQueryParser parser = new FacetQueryParser(this, key);
    return parser.parse(args);
  }

  FacetRange parseRangeFacet(String key, Object args) throws SyntaxError {
    FacetRangeParser parser = new FacetRangeParser(this, key);
    return parser.parse(args);
  }

  public Object parseStringFacetOrStat(String key, String s) throws SyntaxError {
    // "avg(myfield)"
    return parseStringStat(key, s);
    // TODO - simple string representation of facets
  }

  // parses avg(x)
  private AggValueSource parseStringStat(String key, String stat) throws SyntaxError {
    FunctionQParser parser = (FunctionQParser)QParser.getParser(stat, FunctionQParserPlugin.NAME, getSolrRequest());
    AggValueSource agg = parser.parseAgg(FunctionQParser.FLAG_DEFAULT);
    return agg;
  }

  public AggValueSource parseStat(String key, String type, Object args) throws SyntaxError {
    return null;
  }


  public String getField(Map<String,Object> args) {
    Object fieldName = args.get("field"); // TODO: pull out into defined constant
    if (fieldName == null) {
      fieldName = args.get("f");  // short form
    }
    if (fieldName == null) {
      throw err("Missing 'field'");
    }

    if (!(fieldName instanceof String)) {
      throw err("Expected string for 'field', got" + fieldName);
    }

    return (String)fieldName;
  }


  public Long getLongOrNull(Map<String,Object> args, String paramName, boolean required) {
    Object o = args.get(paramName);
    if (o == null) {
      if (required) {
        throw err("Missing required parameter '" + paramName + "'");
      }
      return null;
    }
    if (!(o instanceof Long || o instanceof Integer || o instanceof Short || o instanceof Byte)) {
      throw err("Expected integer type for param '"+paramName + "' but got " + o);
    }

    return ((Number)o).longValue();
  }

  public long getLong(Map<String,Object> args, String paramName, long defVal) {
    Object o = args.get(paramName);
    if (o == null) {
      return defVal;
    }
    if (!(o instanceof Long || o instanceof Integer || o instanceof Short || o instanceof Byte)) {
      throw err("Expected integer type for param '"+paramName + "' but got " + o.getClass().getSimpleName() + " = " + o);
    }

    return ((Number)o).longValue();
  }

  public boolean getBoolean(Map<String,Object> args, String paramName, boolean defVal) {
    Object o = args.get(paramName);
    if (o == null) {
      return defVal;
    }
    // TODO: should we be more flexible and accept things like "true" (strings)?
    // Perhaps wait until the use case comes up.
    if (!(o instanceof Boolean)) {
      throw err("Expected boolean type for param '"+paramName + "' but got " + o.getClass().getSimpleName() + " = " + o);
    }

    return (Boolean)o;
  }

  public String getString(Map<String,Object> args, String paramName, String defVal) {
    Object o = args.get(paramName);
    if (o == null) {
      return defVal;
    }
    if (!(o instanceof String)) {
      throw err("Expected string type for param '"+paramName + "' but got " + o.getClass().getSimpleName() + " = " + o);
    }

    return (String)o;
  }


  public IndexSchema getSchema() {
    return parent.getSchema();
  }

  public SolrQueryRequest getSolrRequest() {
    return parent.getSolrRequest();
  }

}


class FacetTopParser extends FacetParser<FacetQuery> {
  private SolrQueryRequest req;

  public FacetTopParser(SolrQueryRequest req) {
    super(null, "facet");
    this.facet = new FacetQuery();
    this.req = req;
  }

  @Override
  public FacetQuery parse(Object args) throws SyntaxError {
    parseSubs(args);
    return facet;
  }

  @Override
  public SolrQueryRequest getSolrRequest() {
    return req;
  }

  @Override
  public IndexSchema getSchema() {
    return req.getSchema();
  }
}

class FacetQueryParser extends FacetParser<FacetQuery> {
  public FacetQueryParser(FacetParser parent, String key) {
    super(parent, key);
    facet = new FacetQuery();
  }

  @Override
  public FacetQuery parse(Object arg) throws SyntaxError {
    String qstring = null;
    if (arg instanceof String) {
      // just the field name...
      qstring = (String)arg;

    } else if (arg instanceof Map) {
      Map<String, Object> m = (Map<String, Object>) arg;
      qstring = getString(m, "q", null);
      if (qstring == null) {
        qstring = getString(m, "query", null);
      }

      // OK to parse subs before we have parsed our own query?
      // as long as subs don't need to know about it.
      parseSubs( m.get("facet") );
    }

    // TODO: substats that are from defaults!!!

    if (qstring != null) {
      QParser parser = QParser.getParser(qstring, null, getSolrRequest());
      facet.q = parser.getQuery();
    }

    return facet;
  }
}

class FacetFieldParser extends FacetParser<FacetField> {
  public FacetFieldParser(FacetParser parent, String key) {
    super(parent, key);
    facet = new FacetField();
  }

  public FacetField parse(Object arg) throws SyntaxError {

    if (arg instanceof String) {
      // just the field name...
      facet.field = (String)arg;
      parseSort( null );  // TODO: defaults

    } else if (arg instanceof Map) {
      Map<String, Object> m = (Map<String, Object>) arg;
      facet.field = getField(m);
      facet.offset = getLong(m, "offset", facet.offset);
      facet.limit = getLong(m, "limit", facet.limit);
      facet.mincount = getLong(m, "mincount", facet.mincount);
      facet.missing = getBoolean(m, "missing", facet.missing);
      facet.numBuckets = getBoolean(m, "numBuckets", facet.numBuckets);
      facet.prefix = getString(m, "prefix", facet.prefix);
      facet.allBuckets = getBoolean(m, "allBuckets", facet.allBuckets);
      facet.method = FacetField.FacetMethod.fromString(getString(m, "method", null));
      facet.cacheDf = (int)getLong(m, "cacheDf", facet.cacheDf);

      // facet.sort may depend on a facet stat...
      // should we be parsing / validating this here, or in the execution environment?
      Object o = m.get("facet");
      parseSubs(o);

      parseSort( m.get("sort") );
    }

    return facet;
  }


  // Sort specification is currently
  // sort : 'mystat desc'
  // OR
  // sort : { mystat : 'desc' }
  private void parseSort(Object sort) {
    if (sort == null) {
      facet.sortVariable = "count";
      facet.sortDirection = FacetField.SortDirection.desc;
    } else if (sort instanceof String) {
      String sortStr = (String)sort;
      if (sortStr.endsWith(" asc")) {
        facet.sortVariable = sortStr.substring(0, sortStr.length()-" asc".length());
        facet.sortDirection = FacetField.SortDirection.asc;
      } else if (sortStr.endsWith(" desc")) {
        facet.sortVariable = sortStr.substring(0, sortStr.length()-" desc".length());
        facet.sortDirection = FacetField.SortDirection.desc;
      } else {
        facet.sortDirection = "index".equals(facet.sortVariable) ? FacetField.SortDirection.asc : FacetField.SortDirection.desc;  // default direction for "index" is ascending
      }
    } else {
     // sort : { myvar : 'desc' }
      Map<String,Object> map = (Map<String,Object>)sort;
      // TODO: validate
      Map.Entry<String,Object> entry = map.entrySet().iterator().next();
      String k = entry.getKey();
      Object v = entry.getValue();
      facet.sortVariable = k;
      facet.sortDirection = FacetField.SortDirection.valueOf(v.toString());
    }

  }
}



class FacetRangeParser extends FacetParser<FacetRange> {
  public FacetRangeParser(FacetParser parent, String key) {
    super(parent, key);
    facet = new FacetRange();
  }

  public FacetRange parse(Object arg) throws SyntaxError {
    if (!(arg instanceof Map)) {
      throw err("Missing range facet arguments");
    }

    Map<String, Object> m = (Map<String, Object>) arg;

    facet.field = getString(m, "field", null);

    facet.start = m.get("start");
    facet.end = m.get("end");
    facet.gap = m.get("gap");
    facet.hardend = getBoolean(m, "hardend", facet.hardend);

    // TODO: refactor list-of-options code

    Object o = m.get("include");
    String[] includeList = null;
    if (o != null) {
      List lst = null;

      if (o instanceof List) {
        lst = (List)o;
      } else if (o instanceof String) {
        lst = StrUtils.splitSmart((String)o, ',');
      }

      includeList = (String[])lst.toArray(new String[lst.size()]);
    }
    facet.include = FacetParams.FacetRangeInclude.parseParam( includeList );

    facet.others = EnumSet.noneOf(FacetParams.FacetRangeOther.class);

    o = m.get("other");
    if (o != null) {
      List<String> lst = null;

      if (o instanceof List) {
        lst = (List)o;
      } else if (o instanceof String) {
        lst = StrUtils.splitSmart((String)o, ',');
      }

      for (String otherStr : lst) {
        facet.others.add( FacetParams.FacetRangeOther.get(otherStr) );
      }
    }


    Object facetObj = m.get("facet");
    parseSubs(facetObj);

    return facet;
  }

}




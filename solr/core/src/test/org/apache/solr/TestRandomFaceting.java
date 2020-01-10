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
package org.apache.solr;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
public class TestRandomFaceting extends SolrTestCaseJ4 {

  private static final Pattern trieFields = Pattern.compile(".*_t.");

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String FOO_STRING_FIELD = "foo_s1";
  public static final String SMALL_STRING_FIELD = "small_s1";
  public static final String SMALL_INT_FIELD = "small_i";

  @BeforeClass
  public static void beforeTests() throws Exception {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true");
    
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml","schema12.xml");
  }


  int indexSize;
  List<FldType> types;
  Map<Comparable, Doc> model = null;
  boolean validateResponses = true;

  void init() {
    Random rand = random();
    clearIndex();
    model = null;
    indexSize = rand.nextBoolean() ? (rand.nextInt(10) + 1) : (rand.nextInt(100) + 10);

    types = new ArrayList<>();
    types.add(new FldType("id",ONE_ONE, new SVal('A','Z',4,4)));
    types.add(new FldType("score_f",ONE_ONE, new FVal(1,100)));
    types.add(new FldType("small_f",ONE_ONE, new FVal(-4,5)));
    types.add(new FldType("small_d",ONE_ONE, new FVal(-4,5)));
    types.add(new FldType("foo_i",ZERO_ONE, new IRange(-2,indexSize)));
    types.add(new FldType("rare_s1",new IValsPercent(95,0,5,1), new SVal('a','b',1,5)));
    types.add(new FldType("str_s1",ZERO_ONE, new SVal('a','z',1,2)));
    types.add(new FldType("long_s1",ZERO_ONE, new SVal('a','b',1,5)));
    types.add(new FldType("small_s1",ZERO_ONE, new SVal('a',(char)('c'+indexSize/3),1,1)));
    types.add(new FldType("small2_s1",ZERO_ONE, new SVal('a',(char)('c'+indexSize/3),1,1)));
    types.add(new FldType("small2_ss",ZERO_TWO, new SVal('a',(char)('c'+indexSize/3),1,1)));
    types.add(new FldType("small3_ss",new IRange(0,25), new SVal('A','z',1,1)));
    types.add(new FldType("small_i",ZERO_ONE, new IRange(-2,5+indexSize/3)));
    types.add(new FldType("small2_i",ZERO_ONE, new IRange(-1,5+indexSize/3)));
    types.add(new FldType("small2_is",ZERO_TWO, new IRange(-2,5+indexSize/3)));
    types.add(new FldType("small3_is",new IRange(0,25), new IRange(-50,50)));

    types.add(new FldType("missing_i",new IRange(0,0), new IRange(0,100)));
    types.add(new FldType("missing_is",new IRange(0,0), new IRange(0,100)));
    types.add(new FldType("missing_s1",new IRange(0,0), new SVal('a','b',1,1)));
    types.add(new FldType("missing_ss",new IRange(0,0), new SVal('a','b',1,1)));

    // TODO: doubles, multi-floats, ints with precisionStep>0, booleans
    types.add(new FldType("small_tf",ZERO_ONE, new FVal(-4,5)));
    assert trieFields.matcher("small_tf").matches();
    assert !trieFields.matcher("small_f").matches();
    
    types.add(new FldType("foo_ti",ZERO_ONE, new IRange(-2,indexSize)));
    assert trieFields.matcher("foo_ti").matches();
    assert !trieFields.matcher("foo_i").matches();
    
    types.add(new FldType("bool_b",ZERO_ONE, new Vals(){
      @Override
      public Comparable get() {
        return random().nextBoolean();
      }
      
    }));
  }

  void addMoreDocs(int ndocs) throws Exception {
    model = indexDocs(types, model, ndocs);
  }

  void deleteSomeDocs() {
    Random rand = random();
    int percent = rand.nextInt(100);
    if (model == null) return;
    ArrayList<String> ids = new ArrayList<>(model.size());
    for (Comparable id : model.keySet()) {
      if (rand.nextInt(100) < percent) {
        ids.add(id.toString());
      }
    }
    if (ids.size() == 0) return;

    StringBuilder sb = new StringBuilder("id:(");
    for (String id : ids) {
      sb.append(id).append(' ');
      model.remove(id);
    }
    sb.append(')');

    assertU(delQ(sb.toString()));

    if (rand.nextInt(10)==0) {
      assertU(optimize());
    } else {
      assertU(commit("softCommit",""+(rand.nextInt(10)!=0)));
    }
  }

  @Test
  public void testRandomFaceting() throws Exception {
    Random rand = random();
    int iter = atLeast(100);
    init();
    addMoreDocs(0);
    
    for (int i=0; i<iter; i++) {
      doFacetTests();
      
      if (rand.nextInt(100) < 5) {
        init();
      }
      
      addMoreDocs(rand.nextInt(indexSize) + 1);
      
      if (rand.nextInt(100) < 50) {
        deleteSomeDocs();
      }
    }
  }


  void doFacetTests() throws Exception {
    for (FldType ftype : types) {
      doFacetTests(ftype);
    }
  }


  List<String> multiValuedMethods = Arrays.asList(new String[]{"enum","fc", null});
  List<String> singleValuedMethods = Arrays.asList(new String[]{"enum","fc","fcs", null});


  void doFacetTests(FldType ftype) throws Exception {
    SolrQueryRequest req = req();
    try {
      Random rand = random();
      ModifiableSolrParams params = params("facet","true", "wt","json", "indent","true", "omitHeader","true");
      params.add("q","*:*");  // TODO: select subsets
      params.add("rows","0");

      SchemaField sf = req.getSchema().getField(ftype.fname);
      boolean multiValued = sf.getType().multiValuedFieldCache();

      int offset = 0;
      if (rand.nextInt(100) < 20) {
        if (rand.nextBoolean()) {
          offset = rand.nextInt(100) < 10 ? rand.nextInt(indexSize*2) : rand.nextInt(indexSize/3+1);
        }
        params.add("facet.offset", Integer.toString(offset));
      }

      int limit = 100;
      if (rand.nextInt(100) < 20) {
        if (rand.nextBoolean()) {
          limit = rand.nextInt(100) < 10 ? rand.nextInt(indexSize/2+1) : rand.nextInt(indexSize*2);
        }
        params.add("facet.limit", Integer.toString(limit));
      }

      if (rand.nextBoolean()) {
        params.add("facet.sort", rand.nextBoolean() ? "index" : "count");
      }

      if ((ftype.vals instanceof SVal) && rand.nextInt(100) < 20) {
        // validate = false;
        String prefix = ftype.createValue().toString();
        if (rand.nextInt(100) < 5) prefix =  TestUtil.randomUnicodeString(rand);
        else if (rand.nextInt(100) < 10) prefix = Character.toString((char)rand.nextInt(256));
        else if (prefix.length() > 0) prefix = prefix.substring(0, rand.nextInt(prefix.length()));
        params.add("facet.prefix", prefix);
      }

      if (rand.nextInt(100) < 10) {
        params.add("facet.mincount", Integer.toString(rand.nextInt(5)));
      }

      if (rand.nextInt(100) < 20) {
        params.add("facet.missing", "true");
      }

      if (rand.nextBoolean()) {
        params.add("facet.enum.cache.minDf",""+ rand.nextInt(indexSize));
      }
      
      // TODO: randomly add other facet params
      String key = ftype.fname;
      String facet_field = ftype.fname;
      if (random().nextBoolean()) {
        key = "alternate_key";
        facet_field = "{!key="+key+"}"+ftype.fname;
      }
      params.set("facet.field", facet_field);

      List<String> methods = multiValued ? multiValuedMethods : singleValuedMethods;
      List<String> responses = new ArrayList<>(methods.size());
      
      for (String method : methods) {
        for (boolean exists : new boolean[]{false, true}) {
          // params.add("facet.field", "{!key="+method+"}" + ftype.fname);
          // TODO: allow method to be passed on local params?
          if (method!=null) {
            params.set("facet.method", method);
          } else {
            params.remove("facet.method");
          }
          params.set("facet.exists", ""+exists);
          if (!exists && rand.nextBoolean()) {
            params.remove("facet.exists");
          }
          
          // if (random().nextBoolean()) params.set("facet.mincount", "1");  // uncomment to test that validation fails
          if (!(params.getInt("facet.limit", 100) == 0 &&
              !params.getBool("facet.missing", false))) {
            // it bypasses all processing, and we can go to empty validation
            if (exists && params.getInt("facet.mincount", 0)>1) {
              assertQEx("no mincount on facet.exists",
                  rand.nextBoolean() ? "facet.exists":"facet.mincount",
                  req(params), ErrorCode.BAD_REQUEST);
              continue;
            }
            // facet.exists can't be combined with non-enum nor with enum requested for tries, because it will be flipped to FC/FCS
            final boolean notEnum = method != null && !method.equals("enum");
            final boolean trieField = trieFields.matcher(ftype.fname).matches();
            if ((notEnum || trieField) && exists) {
              assertQEx("facet.exists only when enum or ommitted",
                  "facet.exists", req(params), ErrorCode.BAD_REQUEST);
              continue;
            }
            if (exists && sf.getType().isPointField()) {
              // PointFields don't yet support "enum" method or the "facet.exists" parameter
              assertQEx("Expecting failure, since ",
                  "facet.exists=true is requested, but facet.method=enum can't be used with " + sf.getName(),
                  req(params), ErrorCode.BAD_REQUEST);
              continue;
            }
          }
          String strResponse = h.query(req(params));
          responses.add(strResponse);
          
          if (responses.size()>1) {
            validateResponse(responses.get(0), strResponse, params, method, methods);
          }
        }
        
      }
      
      /**
      String strResponse = h.query(req(params));
      Object realResponse = ObjectBuilder.fromJSON(strResponse);
      **/
    } finally {
      req.close();
    }
  }
  private void validateResponse(String expected, String actual, ModifiableSolrParams params, String method,
        List<String> methods) throws Exception {
    if (params.getBool("facet.exists", false)) {
      if (isSortByCount(params)) { // it's challenged with facet.sort=count 
        expected = getExpectationForSortByCount(params, methods);// that requires to recalculate expactation
      } else { // facet.sort=index
        expected = capFacetCountsTo1(expected);
      }
    }
    
    String err = JSONTestUtil.match("/", actual, expected, 0.0);
    if (err != null) {
      log.error("ERROR: mismatch facet response: " + err +
          "\n expected =" + expected +
          "\n response = " + actual +
          "\n request = " + params
      );
      fail(err);
    }
  }

  /** if facet.exists=true with facet.sort=counts,
   * it should return all values with 1 hits ordered by label index
   * then all vals with 0 , and then missing count with null label,
   * in the implementation below they are called three stratas 
   * */
  private String getExpectationForSortByCount( ModifiableSolrParams params, List<String> methods) throws Exception {
    String indexSortedResponse = getIndexSortedAllFacetValues(params, methods);
    
    return transformFacetFields(indexSortedResponse, e -> {
      List<Object> facetSortedByIndex = (List<Object>) e.getValue();
      Map<Integer,List<Object>> stratas = new HashMap<Integer,List<Object>>(){
        @Override // poor man multimap, I won't do that anymore, I swear.
        public List<Object> get(Object key) {
          if (!containsKey(key)) {
            put((Integer) key, new ArrayList<>());
          }
          return super.get(key);
        }
      };
      
      for (Iterator iterator = facetSortedByIndex.iterator(); iterator.hasNext();) {
        Object label = (Object) iterator.next();
        Long count = (Long) iterator.next();
        final Integer strata;
        if (label==null) { // missing (here "stratas" seems like overengineering )
          strata = null;
        }else {
          if (count>0) {
            count = 1L; // capping here 
            strata = 1; // non-zero count become zero
          } else {
            strata = 0; // zero-count
          }
        }
        final List<Object> facet = stratas.get(strata);
        facet.add(label);
        facet.add(count);
      }
      List stratified =new ArrayList<>();
      for(Integer s : new Integer[]{1, 0}) { // non-zero capped to one goes first, zeroes go then
        stratified.addAll(stratas.get(s));
      }// cropping them now
      int offset=params.getInt("facet.offset", 0) * 2;
      int end = offset + params.getInt("facet.limit", 100) * 2 ;
      int fromIndex = offset > stratified.size() ?  stratified.size() : offset;
      stratified = stratified.subList(fromIndex, 
               end > stratified.size() ?  stratified.size() : end);

      stratified.addAll(stratas.get(null));

      facetSortedByIndex.clear();
      facetSortedByIndex.addAll(stratified);
    });
  }

  private String getIndexSortedAllFacetValues(ModifiableSolrParams in, List<String> methods) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams(in);
    params.set("facet.sort", "index");
    String goodOldMethod = methods.get(random().nextInt( methods.size()));
    params.set("facet.method", goodOldMethod);
    params.set("facet.exists", "false");
    if (random().nextBoolean()) {
      params.remove("facet.exists");
    }
    params.set("facet.limit",-1);
    params.set("facet.offset",0);
    final String query;
    SolrQueryRequest req = null;
    try {
      req = req(params);
      query = h.query(req);
    } finally {
      req.close();
    }
    return query;
  }

  private boolean isSortByCount(ModifiableSolrParams in) {
    boolean sortIsCount;
    String sortParam = in.get("facet.sort");
    sortIsCount = "count".equals(sortParam) || (sortParam==null && in.getInt("facet.limit",100)>0);
    return sortIsCount;
  }

  /*
   * {
  "response":{"numFound":6,"start":0,"docs":[]
  },
  "facet_counts":{
    "facet_queries":{},
    "facet_fields":{
      "foo_i":[
        "6",2,
        "2",1,
        "3",1]},
    "facet_ranges":{},
    "facet_intervals":{},
    "facet_heatmaps":{}}} 
   * */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private String capFacetCountsTo1(String expected) throws IOException {
    return transformFacetFields(expected, e -> {
      List<Object> facetValues = (List<Object>) e.getValue();
      for (ListIterator iterator = facetValues.listIterator(); iterator.hasNext();) {
        Object value = iterator.next(); 
        Long count = (Long) iterator.next();
        if (value!=null && count > 1) {
          iterator.set(1);
        }
        
      }
    });
  }
  
  private String transformFacetFields(String expected, Consumer<Map.Entry<Object,Object>> consumer) throws IOException {
    Object json = Utils.fromJSONString(expected);
    Map facet_fields = getFacetFieldMap(json);
    Set entries = facet_fields.entrySet();
    for (Object facetTuples : entries) { //despite there should be only one field
      Entry entry = (Entry)facetTuples;
      consumer.accept(entry);
    }
    return Utils.toJSONString(json);
  }

  private Map getFacetFieldMap(Object json) {
    Object facet_counts = ((Map)json).get("facet_counts");
    Map facet_fields = (Map) ((Map)facet_counts).get("facet_fields");
    return facet_fields;
  }
}



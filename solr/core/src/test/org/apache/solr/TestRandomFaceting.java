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

import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

@Slow
public class TestRandomFaceting extends SolrTestCaseJ4 {

  public static final String FOO_STRING_FIELD = "foo_s1";
  public static final String SMALL_STRING_FIELD = "small_s1";
  public static final String SMALL_INT_FIELD = "small_i";

  @BeforeClass
  public static void beforeTests() throws Exception {
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

    types = new ArrayList<FldType>();
    types.add(new FldType("id",ONE_ONE, new SVal('A','Z',4,4)));
    types.add(new FldType("score_f",ONE_ONE, new FVal(1,100)));
    types.add(new FldType("small_f",ONE_ONE, new FVal(-4,5)));
    types.add(new FldType("small_d",ONE_ONE, new FVal(-4,5)));
    types.add(new FldType("foo_i",ZERO_ONE, new IRange(-2,indexSize)));
    types.add(new FldType("small_s",ZERO_ONE, new SVal('a',(char)('c'+indexSize/3),1,1)));
    types.add(new FldType("small2_s",ZERO_ONE, new SVal('a',(char)('c'+indexSize/3),1,1)));
    types.add(new FldType("small2_ss",ZERO_TWO, new SVal('a',(char)('c'+indexSize/3),1,1)));
    types.add(new FldType("small3_ss",new IRange(0,25), new SVal('A','z',1,1)));
    types.add(new FldType("small_i",ZERO_ONE, new IRange(-2,5+indexSize/3)));
    types.add(new FldType("small2_i",ZERO_ONE, new IRange(-1,5+indexSize/3)));
    types.add(new FldType("small2_is",ZERO_TWO, new IRange(-2,5+indexSize/3)));
    types.add(new FldType("small3_is",new IRange(0,25), new IRange(-50,50)));

    types.add(new FldType("missing_i",new IRange(0,0), new IRange(0,100)));
    types.add(new FldType("missing_is",new IRange(0,0), new IRange(0,100)));
    types.add(new FldType("missing_s",new IRange(0,0), new SVal('a','b',1,1)));
    types.add(new FldType("missing_ss",new IRange(0,0), new SVal('a','b',1,1)));

    // TODO: doubles, multi-floats, ints with precisionStep>0, booleans
  }

  void addMoreDocs(int ndocs) throws Exception {
    model = indexDocs(types, model, ndocs);
  }

  void deleteSomeDocs() {
    Random rand = random();
    int percent = rand.nextInt(100);
    if (model == null) return;
    ArrayList<String> ids = new ArrayList<String>(model.size());
    for (Comparable id : model.keySet()) {
      if (rand.nextInt(100) < percent) {
        ids.add(id.toString());
      }
    }
    if (ids.size() == 0) return;

    StringBuffer sb = new StringBuffer("id:(");
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
    try {
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
    } finally {
      FieldCache.DEFAULT.purgeAllCaches();   // avoid FC insanity
    }
  }


  void doFacetTests() throws Exception {
    for (FldType ftype : types) {
      doFacetTests(ftype);
    }
  }


  List<String> multiValuedMethods = Arrays.asList(new String[]{"enum","fc"});
  List<String> singleValuedMethods = Arrays.asList(new String[]{"enum","fc","fcs"});


  void doFacetTests(FldType ftype) throws Exception {
    SolrQueryRequest req = req();
    try {
      Random rand = random();
      boolean validate = validateResponses;
      ModifiableSolrParams params = params("facet","true", "wt","json", "indent","true", "omitHeader","true");
      params.add("q","*:*", "rows","0");  // TODO: select subsets
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
        if (rand.nextInt(100) < 5) prefix =  _TestUtil.randomUnicodeString(rand);
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

      // TODO: randomly add other facet params
      String key = ftype.fname;
      String facet_field = ftype.fname;
      if (random().nextBoolean()) {
        key = "alternate_key";
        facet_field = "{!key="+key+"}"+ftype.fname;
      }
      params.set("facet.field", facet_field);

      List<String> methods = multiValued ? multiValuedMethods : singleValuedMethods;
      List<String> responses = new ArrayList<String>(methods.size());
      for (String method : methods) {
        // params.add("facet.field", "{!key="+method+"}" + ftype.fname);
        // TODO: allow method to be passed on local params?

        params.set("facet.method", method);

        // if (random().nextBoolean()) params.set("facet.mincount", "1");  // uncomment to test that validation fails

        String strResponse = h.query(req(params));
        // Object realResponse = ObjectBuilder.fromJSON(strResponse);
        // System.out.println(strResponse);

        responses.add(strResponse);
      }

      /**
      String strResponse = h.query(req(params));
      Object realResponse = ObjectBuilder.fromJSON(strResponse);
      **/

      if (validate) {
        for (int i=1; i<methods.size(); i++) {
          String err = JSONTestUtil.match("/", responses.get(i), responses.get(0), 0.0);
          if (err != null) {
            log.error("ERROR: mismatch facet response: " + err +
                "\n expected =" + responses.get(0) +
                "\n response = " + responses.get(i) +
                "\n request = " + params
            );
            fail(err);
          }
        }
      }


    } finally {
      req.close();
    }
  }

}



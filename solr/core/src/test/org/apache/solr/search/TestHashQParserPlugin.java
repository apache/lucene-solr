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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.util.BaseTestHarness;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class TestHashQParserPlugin extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-hash.xml", "schema-hash.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
    clearIndex();
    assertU(commit());
  }


  public int getCost(Random random) {
    int i = random.nextInt(2);
    if(i == 0) {
      return 200;
    } else {
      return 1;
    }
  }

  @Test
  public void testManyHashPartitions() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!hash worker=0 workers=2 cost="+getCost(random())+"}");
    params.add("partitionKeys", "a_i,a_s,a_i,a_s");
    params.add("wt", "xml");
    String response = h.query(req(params));
    BaseTestHarness.validateXPath(response, "//*[@numFound='0']");

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!hash worker=0 workers=2 cost="+getCost(random())+"}");
    params.add("partitionKeys", "a_i,a_s,a_i,a_s,a_i");
    params.add("wt", "xml");
    ModifiableSolrParams finalParams = params;
    expectThrows(SolrException.class, () -> h.query(req(finalParams)));
  }

  @Test
  public void testLessWorkers() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!hash worker=0 workers=1 cost="+getCost(random())+"}");
    params.add("partitionKeys", "a_i");
    params.add("wt", "xml");
    ModifiableSolrParams finalParams = params;
    expectThrows(SolrException.class, () -> h.query(req(finalParams)));
  }

  @Test
  public void testHashPartitionWithEmptyValues() throws Exception {

    assertU(adoc("id", "1", "a_s", "one", "a_i" , "1"));
    assertU(adoc("id", "2", "a_s", "one", "a_i" , "1"));
    assertU(adoc("id", "3"));
    assertU(adoc("id", "4"));
    assertU(commit());

    //Test with string hash
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!hash worker=0 workers=2 cost="+getCost(random())+"}");
    params.add("partitionKeys", "a_s");
    params.add("wt", "xml");
    String response = h.query(req(params));
    BaseTestHarness.validateXPath(response, "//*[@numFound='4']");

    //Test with int hash
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!hash worker=0 workers=2 cost="+getCost(random())+"}");
    params.add("partitionKeys", "a_i");
    params.add("wt", "xml");
    response = h.query(req(params));
    BaseTestHarness.validateXPath(response, "//*[@numFound='4']");
  }


  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testHashPartition() throws Exception {


    Random random = random();
    HashSet<String> set = new HashSet();

    for (int i=0; i<50; i++) {
      int v = random.nextInt(1000000);
      String val = Integer.toString(v);
      if(!set.contains(val)){
        set.add(val);
        String[] doc = {"id", val, "a_s", val, "a_i", val, "a_l", val};
        assertU(adoc(doc));
        if(i % 10 == 0)
        assertU(commit());

      }
    }
    assertU(commit());


    //Test with 3 worker and String hash ID.

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!hash worker=0 workers=3 cost="+getCost(random)+"}");
    params.add("partitionKeys", "a_s");
    params.add("rows","50");
    params.add("wt", "xml");
    HashSet set1 = new HashSet();
    String response = h.query(req(params));

    Iterator<String> it = set.iterator();

    while(it.hasNext()) {
      String s = it.next();
      String results = BaseTestHarness.validateXPath(response, "*[count(//str[@name='id'][.='"+s+"'])=1]");
      if(results == null) {
        set1.add(s);
      }
    }

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!hash worker=1 workers=3 cost="+getCost(random)+"}");
    params.add("partitionKeys", "a_s");
    params.add("rows","50");
    params.add("wt", "xml");
    HashSet set2 = new HashSet();
    response = h.query(req(params));

    it = set.iterator();

    while(it.hasNext()) {
      String s = it.next();
      String results = BaseTestHarness.validateXPath(response, "*[count(//str[@name='id'][.='"+s+"'])=1]");
      if(results == null) {
        set2.add(s);
      }
    }


    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!hash worker=2 workers=3 cost="+getCost(random)+"}");
    params.add("partitionKeys", "a_s");
    params.add("rows","50");
    params.add("wt", "xml");
    HashSet set3 = new HashSet();
    response = h.query(req(params));

    it = set.iterator();

    while(it.hasNext()) {
      String s = it.next();
      String results = BaseTestHarness.validateXPath(response, "*[count(//str[@name='id'][.='"+s+"'])=1]");
      if(results == null) {
        set3.add(s);
      }
    }

    assert(set1.size() > 0);
    assert(set2.size() > 0);
    assert(set3.size() > 0);
    assert(set1.size()+set2.size()+set3.size()==set.size());
    assertNoOverLap(set1, set2);
    assertNoOverLap(set1, set3);
    assertNoOverLap(set2, set3);


    //Test with 2 workers and int partition Key


    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!hash worker=0 workers=2 cost="+getCost(random)+"}");
    params.add("partitionKeys", "a_i");
    params.add("rows","50");
    params.add("wt", "xml");
    set1 = new HashSet();
    response = h.query(req(params));

    it = set.iterator();

    while(it.hasNext()) {
      String s = it.next();
      String results = BaseTestHarness.validateXPath(response, "*[count(//str[@name='id'][.='"+s+"'])=1]");
      if(results == null) {
        set1.add(s);
      }
    }

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!hash worker=1 workers=2 cost="+getCost(random)+"}");
    params.add("partitionKeys", "a_i");
    params.add("rows","50");
    params.add("wt", "xml");
    set2 = new HashSet();
    response = h.query(req(params));

    it = set.iterator();

    while(it.hasNext()) {
      String s = it.next();
      String results = BaseTestHarness.validateXPath(response, "*[count(//str[@name='id'][.='"+s+"'])=1]");
      if(results == null) {
        set2.add(s);
      }
    }

    assert(set1.size() > 0);
    assert(set2.size() > 0);
    assert(set1.size()+set2.size()==set.size());
    assertNoOverLap(set1, set2);


    //Test with 2 workers and compound partition Key


    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!hash worker=0 workers=2 cost="+getCost(random)+"}");
    params.add("partitionKeys", "a_s,       a_i,      a_l");
    params.add("rows","50");
    params.add("wt", "xml");
    set1 = new HashSet();
    response = h.query(req(params));

    it = set.iterator();

    while(it.hasNext()) {
      String s = it.next();
      String results = BaseTestHarness.validateXPath(response, "*[count(//str[@name='id'][.='"+s+"'])=1]");
      if(results == null) {
        set1.add(s);
      }
    }

    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!hash worker=1 workers=2 cost="+getCost(random)+"}");
    params.add("partitionKeys", "a_s, a_i, a_l");
    params.add("rows","50");
    params.add("wt", "xml");
    set2 = new HashSet();
    response = h.query(req(params));

    it = set.iterator();

    while(it.hasNext()) {
      String s = it.next();
      String results = BaseTestHarness.validateXPath(response, "*[count(//str[@name='id'][.='"+s+"'])=1]");
      if(results == null) {
        set2.add(s);
      }
    }

    assert(set1.size() > 0);
    assert(set2.size() > 0);
    assert(set1.size()+set2.size()==set.size());
    assertNoOverLap(set1, set2);
  }


  private void assertNoOverLap(@SuppressWarnings({"rawtypes"})Set setA,
                               @SuppressWarnings({"rawtypes"})Set setB) throws Exception {
    @SuppressWarnings({"rawtypes"})
    Iterator it =  setA.iterator();
    while(it.hasNext()) {
      Object o = it.next();
      if(setB.contains(o)) {
        throw new Exception("Overlapping sets for value:"+o.toString());
      }
    }
  }
}

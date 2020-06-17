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
package org.apache.solr.common.util;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public class TestNamedListCodec  extends SolrTestCase {
  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testSimple() throws Exception{

    NamedList nl = new NamedList();
    Float fval = 10.01f;
    Boolean bval = Boolean.TRUE;
    String sval = "12qwaszx";

    // Set up a simple document
    NamedList r = new NamedList();


    nl.add("responseHeader", r);

    r.add("status",0);
    r.add("QTime",63);
    NamedList p = new NamedList();
    r.add("params",p);
    p.add("rows",10);
    p.add("start",0);
    p.add("indent","on");
    p.add("q","ipod");


    SolrDocumentList list =     new SolrDocumentList();
    nl.add("response", list );
    list.setMaxScore(1.0f);
    list.setStart(10);
    list.setNumFound(12);

    SolrDocument doc = new SolrDocument();
    doc.addField( "f", fval );
    doc.addField( "b", bval );
    doc.addField( "s", sval );
    doc.addField( "f", 100 );
    list.add(doc);

    doc = new SolrDocument();
    doc.addField( "f", fval );
    doc.addField( "b", bval );
    doc.addField( "s", sval );
    doc.addField( "f", 101 );
    list.add(doc);

    nl.add("zzz",doc);
    byte[] arr;
    try (JavaBinCodec jbc = new JavaBinCodec(null); ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      jbc.marshal(nl, baos);
      arr = baos.toByteArray();
    }
    try (JavaBinCodec jbc = new JavaBinCodec(); ByteArrayInputStream bais = new ByteArrayInputStream(arr)) {
      nl = (NamedList) jbc.unmarshal(bais);
    }


    assertEquals(3, nl.size());
    assertEquals( "ipod",((NamedList)((NamedList)nl.getVal(0)).get("params")).get("q") );
    list = (SolrDocumentList) nl.getVal(1);
    assertEquals(12,list.getNumFound() );
    assertEquals(10,list.getStart() );
    assertEquals(101, ((List)list.get(1).getFieldValue("f")).get(1));
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  @SuppressWarnings({"unchecked"})
  public void testIterator() throws Exception{
    
    @SuppressWarnings({"rawtypes"})
    NamedList nl = new NamedList();
    Float fval = 10.01f;
    Boolean bval = Boolean.TRUE;
    String sval = "12qwaszx";

    // Set up a simple document
    @SuppressWarnings({"rawtypes"})
    NamedList r = new NamedList();
    @SuppressWarnings({"rawtypes"})
    List list =     new ArrayList();

    SolrDocument doc = new SolrDocument();
    doc.addField( "f", fval );
    doc.addField( "b", bval );
    doc.addField( "s", sval );
    doc.addField( "f", 100 );
    list.add(doc);

    doc = new SolrDocument();
    doc.addField( "f", fval );
    doc.addField( "b", bval );
    doc.addField( "s", sval );
    doc.addField( "f", 101 );
    list.add(doc);

    nl.add("zzz",list.iterator());
    byte[] arr;
    try (JavaBinCodec jbc = new JavaBinCodec(null); ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      jbc.marshal(nl, baos);
      arr = baos.toByteArray();
    }
    try (JavaBinCodec jbc = new JavaBinCodec(); ByteArrayInputStream bais = new ByteArrayInputStream(arr)) {
      nl = (NamedList) jbc.unmarshal(bais);
    }

    @SuppressWarnings({"rawtypes"})
    List l = (List) nl.get("zzz");
    assertEquals(list.size(), l.size());
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  @SuppressWarnings({"unchecked"})
  public void testIterable() throws Exception {
    

    @SuppressWarnings({"rawtypes"})
    NamedList r = new NamedList();

    Map<String, String> map = new HashMap<>();
    map.put("foo", "bar");
    map.put("junk", "funk");
    map.put("ham", "burger");

    r.add("keys", map.keySet());
    r.add("more", "less");
    r.add("values", map.values());
    r.add("finally", "the end");
    byte[] arr;
    try (JavaBinCodec jbc = new JavaBinCodec(null); ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      jbc.marshal(r,baos);
      arr = baos.toByteArray();
    }

    try (JavaBinCodec jbc = new JavaBinCodec(); ByteArrayInputStream bais = new ByteArrayInputStream(arr)) {
      @SuppressWarnings({"rawtypes"})
      NamedList result = (NamedList) jbc.unmarshal(bais);
      assertTrue("result is null and it shouldn't be", result != null);
      @SuppressWarnings({"rawtypes"})
      List keys = (List) result.get("keys");
      assertTrue("keys is null and it shouldn't be", keys != null);
      assertTrue("keys Size: " + keys.size() + " is not: " + 3, keys.size() == 3);
      String less = (String) result.get("more");
      assertTrue("less is null and it shouldn't be", less != null);
      assertTrue(less + " is not equal to " + "less", less.equals("less") == true);
      @SuppressWarnings({"rawtypes"})
      List values = (List) result.get("values");
      assertTrue("values is null and it shouldn't be", values != null);
      assertTrue("values Size: " + values.size() + " is not: " + 3, values.size() == 3);
      String theEnd = (String) result.get("finally");
      assertTrue("theEnd is null and it shouldn't be", theEnd != null);
      assertTrue(theEnd + " is not equal to " + "the end", theEnd.equals("the end") == true);
    } catch (ClassCastException e) {
      assertTrue("Received a CCE and we shouldn't have", false);
    }

  }


  int rSz(int orderOfMagnitude) {
    int sz = r.nextInt(orderOfMagnitude);
    switch (sz) {
      case 0: return r.nextInt(10);
      case 1: return r.nextInt(100);
      case 2: return r.nextInt(1000);
      default: return r.nextInt(10000);
    }
  }

  public String rStr(int sz) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<sz; i++) {
      sb.appendCodePoint(r.nextInt(Character.MIN_HIGH_SURROGATE));
    }
    return sb.toString();
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  public NamedList rNamedList(int lev) {
    int sz = lev<= 0 ? 0 : r.nextInt(3);
    NamedList nl = new NamedList();
    for (int i=0; i<sz; i++) {
      nl.add(rStr(2), makeRandom(lev-1));
    }
    return nl;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public List rList(int lev) {
    int sz = lev<= 0 ? 0 : r.nextInt(3);
    ArrayList lst = new ArrayList();
    for (int i=0; i<sz; i++) {
      lst.add(makeRandom(lev-1));
    }
    return lst;
  }

  Random r;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    r = random();
  }

  public Object makeRandom(int lev) {
    switch (r.nextInt(10)) {
      case 0:
        return rList(lev);
      case 1:
        return rNamedList(lev);
      case 2:
        return rStr(rSz(4));
      case 3:
        return r.nextInt();
      case 4:
        return r.nextLong();
      case 5:
        return r.nextBoolean();
      case 6:
        byte[] arr = new byte[rSz(4)];
        r.nextBytes(arr);
        return arr;
      case 7:
        return r.nextFloat();
      case 8:
        return r.nextDouble();
      default:
        return null;
    }
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testRandom() throws Exception {
    // Random r = random;
    // let's keep it deterministic since just the wrong
    // random stuff could cause failure because of an OOM (too big)

    @SuppressWarnings({"rawtypes"})
    NamedList nl;
    @SuppressWarnings({"rawtypes"})
    NamedList res;
    String cmp;

    for (int i=0; i<10000; i++) { // pump up the iterations for good stress testing
      nl = rNamedList(3);
      byte[] arr;
      try (JavaBinCodec jbc = new JavaBinCodec(); ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        jbc.marshal(nl, baos);
        arr = baos.toByteArray();
      }
      // System.out.println(arr.length);
      try (JavaBinCodec jbc = new JavaBinCodec(); ByteArrayInputStream bais = new ByteArrayInputStream(arr)) {
        res = (NamedList) jbc.unmarshal(bais);
        cmp = BaseDistributedSearchTestCase.compare(nl, res, 0, null);
      }
      if (cmp != null) {
        System.out.println(nl);
        System.out.println(res);
        fail(cmp);
      }
    }
  }
}

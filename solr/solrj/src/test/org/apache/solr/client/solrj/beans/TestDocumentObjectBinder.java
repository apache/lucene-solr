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
package org.apache.solr.client.solrj.beans;

import org.apache.lucene.util.LuceneTestCase;

import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestDocumentObjectBinder extends LuceneTestCase {

  public void testSimple() throws Exception {
    DocumentObjectBinder binder = new DocumentObjectBinder();
    XMLResponseParser parser = new XMLResponseParser();
    NamedList<Object> nl = parser.processResponse(new StringReader(xml));
    QueryResponse res = new QueryResponse(nl, null);

    SolrDocumentList solDocList = res.getResults();
    List<Item> l = binder.getBeans(Item.class,res.getResults());
    assertEquals(solDocList.size(), l.size());
    assertEquals(solDocList.get(0).getFieldValue("features"), l.get(0).features);

    Item item = new Item();
    item.id = "aaa";
    item.categories = new String[] {"aaa", "bbb", "ccc"};
    SolrInputDocument out = binder.toSolrInputDocument(item);

    assertEquals(item.id, out.getFieldValue("id"));
    SolrInputField catfield = out.getField("cat");
    assertEquals(3, catfield.getValueCount());

    List<String> catValues = (List<String>) catfield.getValue();
    assertEquals("aaa", catValues.get(0));
    assertEquals("bbb", catValues.get(1));
    assertEquals("ccc", catValues.get(2));
  }

  @Test(expected = BindingException.class)
  public void testNoGetterError() {
    NotGettableItem notGettableItem = new NotGettableItem();
    notGettableItem.setInStock(false);
    new DocumentObjectBinder().toSolrInputDocument(notGettableItem);
  }

  public void testSingleVal4Array() {
    DocumentObjectBinder binder = new DocumentObjectBinder();
    SolrDocumentList solDocList = new SolrDocumentList();
    SolrDocument d = new SolrDocument();
    solDocList.add(d);
    d.setField("cat", "hello");
    List<Item> l = binder.getBeans(Item.class, solDocList);
    assertEquals("hello", l.get(0).categories[0]);
  }

  public void testDynamicFieldBinding() {
    DocumentObjectBinder binder = new DocumentObjectBinder();
    XMLResponseParser parser = new XMLResponseParser();
    NamedList<Object> nl = parser.processResponse(new StringReader(xml));
    QueryResponse res = new QueryResponse(nl, null);
    List<Item> l = binder.getBeans(Item.class,res.getResults());

    assertArrayEquals(new String[]{"Mobile Store", "iPod Store", "CCTV Store"}, l.get(3).getAllSuppliers());
    assertTrue(l.get(3).supplier.containsKey("supplier_1"));
    assertTrue(l.get(3).supplier.containsKey("supplier_2"));
    assertEquals(2, l.get(3).supplier.size());

    List<String> supplierOne = l.get(3).supplier.get("supplier_1");
    assertEquals("Mobile Store", supplierOne.get(0));
    assertEquals("iPod Store", supplierOne.get(1));

    List<String> supplierTwo = l.get(3).supplier.get("supplier_2");
    assertEquals("CCTV Store", supplierTwo.get(0));
  }

  public void testToAndFromSolrDocument() {
    Item item = new Item();
    item.id = "one";
    item.inStock = false;
    item.categories = new String[] {"aaa", "bbb", "ccc"};
    item.features = Arrays.asList(item.categories);
    List<String> supA = Arrays.asList("supA1", "supA2", "supA3");
    List<String> supB = Arrays.asList("supB1", "supB2", "supB3");
    item.supplier = new HashMap<String, List<String>>();
    item.supplier.put("supplier_supA", supA);
    item.supplier.put("supplier_supB", supB);
    
    item.supplier_simple = new HashMap<String, String>();
    item.supplier_simple.put("sup_simple_supA", "supA_val");
    item.supplier_simple.put("sup_simple_supB", "supB_val");
    
    DocumentObjectBinder binder = new DocumentObjectBinder();
    SolrInputDocument doc = binder.toSolrInputDocument(item);
    SolrDocumentList docs = new SolrDocumentList();
    docs.add(ClientUtils.toSolrDocument(doc));
    Item out = binder.getBeans(Item.class, docs).get(0);
    Item singleOut = binder.getBean(Item.class, ClientUtils.toSolrDocument(doc));
    
    // make sure it came out the same
    assertEquals(item.id, out.id);
    assertEquals(item.inStock, out.inStock);
    assertEquals(item.categories.length, out.categories.length);
    assertEquals(item.features, out.features);
    assertEquals(supA, out.supplier.get("supplier_supA"));
    assertEquals(supB, out.supplier.get("supplier_supB"));
    assertEquals(item.supplier_simple.get("sup_simple_supB"), out.supplier_simple.get("sup_simple_supB"));

    assertEquals(item.id, singleOut.id);
    assertEquals(item.inStock, singleOut.inStock);
    assertEquals(item.categories.length, singleOut.categories.length);
    assertEquals(item.features, singleOut.features);
    assertEquals(supA, singleOut.supplier.get("supplier_supA"));
    assertEquals(supB, singleOut.supplier.get("supplier_supB"));
    assertEquals(item.supplier_simple.get("sup_simple_supB"), out.supplier_simple.get("sup_simple_supB"));
    
//    put back "out" as Bean, to see if both ways work as you would expect
//    but the Field that "allSuppliers" need to be cleared, as it is just for 
//    retrieving data, not to post data
    out.allSuppliers = null;
    SolrInputDocument doc1 = binder.toSolrInputDocument(out);
    
    SolrDocumentList docs1 = new SolrDocumentList();
    docs1.add(ClientUtils.toSolrDocument(doc1));
    Item out1 = binder.getBeans(Item.class, docs1).get(0);

    assertEquals(item.id, out1.id);
    assertEquals(item.inStock, out1.inStock);
    assertEquals(item.categories.length, out1.categories.length);
    assertEquals(item.features, out1.features);

    assertEquals(item.supplier_simple.get("sup_simple_supB"), out1.supplier_simple.get("sup_simple_supB"));

    assertEquals(supA, out1.supplier.get("supplier_supA"));
    assertEquals(supB, out1.supplier.get("supplier_supB"));
  }

  public static class Item {
    @Field
    String id;

    @Field("cat")
    String[] categories;

    @Field
    List<String> features;

    @Field
    Date timestamp;

    @Field("highway_mileage")
    int mwyMileage;

    boolean inStock;

    @Field("supplier_*")
    Map<String, List<String>> supplier;
    
    @Field("sup_simple_*")
    Map<String, String> supplier_simple;

    private String[] allSuppliers;

    @Field("supplier_*")
    public void setAllSuppliers(String[] allSuppliers) {
      this.allSuppliers = allSuppliers;  
    }

    public String[] getAllSuppliers() {
      return this.allSuppliers;
    }

    @Field
    public void setInStock(Boolean b) {
      inStock = b;
    }
    
    // required if you want to fill SolrDocuments with the same annotaion...
    public boolean isInStock() {
      return inStock;
    }
  }
  

  public static class NotGettableItem {
    @Field
    String id;

    private boolean inStock;
    private String aaa;

    @Field
    public void setInStock(Boolean b) {
      inStock = b;
    }

    public String getAaa() {
      return aaa;
    }

    @Field
    public void setAaa(String aaa) {
      this.aaa = aaa;
    }
  }

  public static final String xml = 
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
    "<response>" +
    "<lst name=\"responseHeader\"><int name=\"status\">0</int><int name=\"QTime\">0</int><lst name=\"params\"><str name=\"start\">0</str><str name=\"q\">*:*\n" +
    "</str><str name=\"version\">2.2</str><str name=\"rows\">4</str></lst></lst><result name=\"response\" numFound=\"26\" start=\"0\"><doc><arr name=\"cat\">" +
    "<str>electronics</str><str>hard drive</str></arr><arr name=\"features\"><str>7200RPM, 8MB cache, IDE Ultra ATA-133</str>" +
    "<str>NoiseGuard, SilentSeek technology, Fluid Dynamic Bearing (FDB) motor</str></arr><str name=\"id\">SP2514N</str>" +
    "<bool name=\"inStock\">true</bool><str name=\"manu\">Samsung Electronics Co. Ltd.</str><str name=\"name\">Samsung SpinPoint P120 SP2514N - hard drive - 250 GB - ATA-133</str>" +
    "<int name=\"popularity\">6</int><float name=\"price\">92.0</float><str name=\"sku\">SP2514N</str><date name=\"timestamp\">2008-04-16T10:35:57.078Z</date></doc>" +
    "<doc><arr name=\"cat\"><str>electronics</str><str>hard drive</str></arr><arr name=\"features\"><str>SATA 3.0Gb/s, NCQ</str><str>8.5ms seek</str>" +
    "<str>16MB cache</str></arr><str name=\"id\">6H500F0</str><bool name=\"inStock\">true</bool><str name=\"manu\">Maxtor Corp.</str>" +
    "<str name=\"name\">Maxtor DiamondMax 11 - hard drive - 500 GB - SATA-300</str><int name=\"popularity\">6</int><float name=\"price\">350.0</float>" +
    "<str name=\"sku\">6H500F0</str><date name=\"timestamp\">2008-04-16T10:35:57.109Z</date></doc><doc><arr name=\"cat\"><str>electronics</str>" +
    "<str>connector</str></arr><arr name=\"features\"><str>car power adapter, white</str></arr><str name=\"id\">F8V7067-APL-KIT</str>" +
    "<bool name=\"inStock\">false</bool><str name=\"manu\">Belkin</str><str name=\"name\">Belkin Mobile Power Cord for iPod w/ Dock</str>" +
    "<int name=\"popularity\">1</int><float name=\"price\">19.95</float><str name=\"sku\">F8V7067-APL-KIT</str>" +
    "<date name=\"timestamp\">2008-04-16T10:35:57.140Z</date><float name=\"weight\">4.0</float></doc><doc>" +
    "<arr name=\"cat\"><str>electronics</str><str>connector</str></arr><arr name=\"features\">" +
    "<str>car power adapter for iPod, white</str></arr><str name=\"id\">IW-02</str><bool name=\"inStock\">false</bool>" +
    "<str name=\"manu\">Belkin</str><str name=\"name\">iPod &amp; iPod Mini USB 2.0 Cable</str>" +
    "<int name=\"popularity\">1</int><float name=\"price\">11.5</float><str name=\"sku\">IW-02</str>" +
    "<str name=\"supplier_1\">Mobile Store</str><str name=\"supplier_1\">iPod Store</str><str name=\"supplier_2\">CCTV Store</str>" +
    "<date name=\"timestamp\">2008-04-16T10:35:57.140Z</date><float name=\"weight\">2.0</float></doc></result>\n" +
    "</response>";
}

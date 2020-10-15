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
package org.apache.solr.client.solrj.beans;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

import java.io.StringReader;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;


public class TestDocumentObjectBinder extends SolrTestCase {

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
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

    @SuppressWarnings({"unchecked"})
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

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
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

  public void testChild() throws Exception {
    SingleValueChild in = new SingleValueChild();
    in.id = "1";
    in.child = new Child();
    in.child.id = "1.0";
    in.child.name = "Name One";
    DocumentObjectBinder binder = new DocumentObjectBinder();
    SolrInputDocument solrInputDoc = binder.toSolrInputDocument(in);
    SolrDocument solrDoc = toSolrDocument(solrInputDoc);
    assertEquals(1, solrInputDoc.getChildDocuments().size());
    assertEquals(1, solrDoc.getChildDocuments().size());
    SingleValueChild out = binder.getBean(SingleValueChild.class, solrDoc);
    assertEquals(in.id, out.id);
    assertEquals(in.child.id, out.child.id);
    assertEquals(in.child.name, out.child.name);

    ListChild listIn = new ListChild();
    listIn.id = "2";
    Child child = new Child();
    child.id = "1.1";
    child.name = "Name Two";
    listIn.child = Arrays.asList(in.child, child);
    solrInputDoc = binder.toSolrInputDocument(listIn);
    solrDoc = toSolrDocument(solrInputDoc);
    assertEquals(2, solrInputDoc.getChildDocuments().size());
    assertEquals(2, solrDoc.getChildDocuments().size());
    ListChild listOut = binder.getBean(ListChild.class, solrDoc);
    assertEquals(listIn.id, listOut.id);
    assertEquals(listIn.child.get(0).id, listOut.child.get(0).id);
    assertEquals(listIn.child.get(0).name, listOut.child.get(0).name);
    assertEquals(listIn.child.get(1).id, listOut.child.get(1).id);
    assertEquals(listIn.child.get(1).name, listOut.child.get(1).name);

    ArrayChild arrIn = new ArrayChild();
    arrIn.id = "3";
    arrIn.child = new Child[]{in.child, child};
    solrInputDoc = binder.toSolrInputDocument(arrIn);
    solrDoc = toSolrDocument(solrInputDoc);
    assertEquals(2, solrInputDoc.getChildDocuments().size());
    assertEquals(2, solrDoc.getChildDocuments().size());
    ArrayChild arrOut = binder.getBean(ArrayChild.class, solrDoc);
    assertEquals(arrIn.id, arrOut.id);
    assertEquals(arrIn.child[0].id, arrOut.child[0].id);
    assertEquals(arrIn.child[0].name, arrOut.child[0].name);
    assertEquals(arrIn.child[1].id, arrOut.child[1].id);
    assertEquals(arrIn.child[1].name, arrOut.child[1].name);

  }

  private static SolrDocument toSolrDocument(SolrInputDocument d) {
    SolrDocument doc = new SolrDocument();
    for (SolrInputField field : d) {
      doc.setField(field.getName(), field.getValue());
    }
    if (d.getChildDocuments() != null) {
      for (SolrInputDocument in : d.getChildDocuments()) {
        doc.addChildDocument(toSolrDocument(in));
      }
    }
    return doc;
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

  public static class Child {
    @Field
    String id;

    @Field
    String name;

  }

  public static class SingleValueChild {
    @Field
    String id;

    @Field(child = true)
    Child child;
  }

  public static class ListChild {
    @Field
    String id;

    @Field(child = true)
    List<Child> child;

  }

  public static class ArrayChild {

    @Field
    String id;

    @Field(child = true)
    Child[] child;
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

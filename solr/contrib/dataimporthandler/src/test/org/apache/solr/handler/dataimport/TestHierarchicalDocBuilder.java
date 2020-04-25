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
package org.apache.solr.handler.dataimport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.dataimport.config.ConfigNameConstants;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.TestHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for DocBuilder using the test harness. 
 * <b> Documents are hierarchical in this test, i.e. each document have nested children documents.</b>
 */
public class TestHierarchicalDocBuilder extends AbstractDataImportHandlerTestCase {

  private static final String FIELD_ID = "id";
  private int id = 0; //unique id
  private SolrQueryRequest req;
  
  /**
   * Holds the data related to randomly created index.
   * It is used for making assertions.
   */
  private static class ContextHolder {
    /** Overall documents number **/
    int counter = 0;
    
    /**
     * Each Hierarchy object represents nested documents with a parent at the root of hierarchy
     */
    List<Hierarchy> hierarchies = new ArrayList<Hierarchy>();
  }
  
  /**
   * Represents a hierarchical document structure
   */
  private static class Hierarchy {
    
    /**
     * Type of element, i.e. parent, child, grandchild, etc..
     */
    String elementType;
    
    /**
     * Fields of a current element
     */
    Map<String, Object> elementData = new HashMap<String,Object>();
    
    /**
     * Nested elements/documents hierarchies. 
     */
    List<Hierarchy> elements = new ArrayList<Hierarchy>();
  }
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("dataimport-solrconfig.xml", "dataimport-schema.xml");    
  }
  
  @Before
  public void before() {
    req = req("*:*"); // don't really care about query
    MockDataSource.clearCache();
  }
  
  @After
  public void after() {
    if (null != req) {
      req.close();
      req = null;
    }
    MockDataSource.clearCache();
  }

  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-12801") // this test fails easily under beasting
  public void testThreeLevelHierarchy() throws Exception {
    int parentsNum = 3; //fixed for simplicity of test
    int childrenNum = 0;
    int grandChildrenNum = 0;
    
    final String parentType = "parent";
    final String childType = "child";
    final String grandChildType = "grand_child";

    List<String> parentIds = createDataIterator("select * from PARENT", parentType, parentType, parentsNum);
    Collections.shuffle(parentIds, random());
    final String parentId1 = parentIds.get(0);
    String parentId2 = parentIds.get(1);
    
    //parent 1 children
    int firstParentChildrenNum = 3; //fixed for simplicity of test
    String select = "select * from CHILD where parent_id='" + parentId1 + "'";
    List<String> childrenIds = createDataIterator(select, childType, "child of first parent", firstParentChildrenNum);
    List<String> firstParentChildrenIds = new ArrayList<String>(childrenIds);
    childrenNum += childrenIds.size();
    
    // grand children of first parent first child
    final String childId = childrenIds.get(0);
    String description = "grandchild of first parent, child of " + childId + " child";
    select = "select * from GRANDCHILD where parent_id='" + childId + "'";
    List<String> grandChildrenIds = createDataIterator(select, grandChildType, description, atLeast(2));
    grandChildrenNum += grandChildrenIds.size();
    
    // grand children of first parent second child
    {
      String childId2 = childrenIds.get(1);
      description = "grandchild of first parent, child of " + childId2 + " child";
      select = "select * from GRANDCHILD where parent_id='" + childId2 + "'";
    }
    final List<String> grandChildrenIds2 = createDataIterator(select, grandChildType, description, atLeast(2));
    grandChildrenNum += grandChildrenIds2.size();
    
    List<String> allGrandChildrenIds = new ArrayList<>(grandChildrenIds);
    allGrandChildrenIds.addAll(grandChildrenIds2);
        
    // third children of first parent has no grand children
    
    // parent 2 children (no grand children)   
    select = "select * from CHILD where parent_id='" + parentId2 + "'";
    childrenIds = createDataIterator(select, childType, "child of second parent", atLeast(2));
    childrenNum += childrenIds.size();
    
    // parent 3 has no children and grand children
    
    int totalDocsNum = parentsNum + childrenNum + grandChildrenNum;
    
    String resp = runFullImport(THREE_LEVEL_HIERARCHY_CONFIG);
    String xpath = "//arr[@name='documents']/lst[arr[@name='id']/str='"+parentId1+"']/"+
      "arr[@name='_childDocuments_']/lst[arr[@name='id']/str='"+childId+"']/"+
      "arr[@name='_childDocuments_']/lst[arr[@name='id']/str='"+grandChildrenIds.get(0)+"']";
    String results = TestHarness.validateXPath(resp, 
           xpath);
    assertTrue("Debug documents does not contain child documents\n"+resp+"\n"+ xpath+
                                                        "\n"+results, results == null);
    
    assertTrue("Update request processor processAdd was not called", TestUpdateRequestProcessor.processAddCalled);
    assertTrue("Update request processor processCommit was not callled", TestUpdateRequestProcessor.processCommitCalled);
    assertTrue("Update request processor finish was not called", TestUpdateRequestProcessor.finishCalled);
    
    // very simple asserts to check that we at least have correct num of docs indexed
    assertQ(req("*:*"), "//*[@numFound='" + totalDocsNum + "']");
    assertQ(req("type_s:parent"), "//*[@numFound='" + parentsNum + "']");
    assertQ(req("type_s:child"), "//*[@numFound='" + childrenNum + "']");
    assertQ(req("type_s:grand_child"), "//*[@numFound='" + grandChildrenNum + "']");

    // let's check BlockJoin
    // get first parent by any grand children
    String randomGrandChildId = allGrandChildrenIds.get(random().nextInt(allGrandChildrenIds.size()));
    Query query = createToParentQuery(parentType, FIELD_ID, randomGrandChildId);
    assertSearch(query, FIELD_ID, parentId1);

    // get first parent by any children 
    String randomChildId = firstParentChildrenIds.get(random().nextInt(firstParentChildrenIds.size()));
    query = createToParentQuery(parentType, FIELD_ID, randomChildId);
    assertSearch(query, FIELD_ID, parentId1);
    
    // get parent by children by grand children
    randomGrandChildId = grandChildrenIds.get(random().nextInt(grandChildrenIds.size()));
    ToParentBlockJoinQuery childBlockJoinQuery = createToParentQuery(childType, FIELD_ID, randomGrandChildId);
    ToParentBlockJoinQuery blockJoinQuery = new ToParentBlockJoinQuery(childBlockJoinQuery, createParentFilter(parentType), ScoreMode.Avg);
    assertSearch(blockJoinQuery, FIELD_ID, parentId1);
  }

  @Test
  public void testRandomDepthHierarchy() throws Exception {
    final String parentType = "parent";
    
    // Be aware that hierarchies grows exponentially, thus 
    // numbers bigger than 6 may lead to significant memory usage
    // and cause OOME
    int parentsNum = 2 + random().nextInt(3);
    int depth = 2 + random().nextInt(3);
    
    ContextHolder holder = new ContextHolder();
    
    String config = createRandomizedConfig(depth, parentType, parentsNum, holder);
    runFullImport(config);
    
    assertTrue("Update request processor processAdd was not called", TestUpdateRequestProcessor.processAddCalled);
    assertTrue("Update request processor processCommit was not callled", TestUpdateRequestProcessor.processCommitCalled);
    assertTrue("Update request processor finish was not called", TestUpdateRequestProcessor.finishCalled);
    
    assertQ(req("type_s:" + parentType), "//*[@numFound='" + parentsNum + "']");
    assertQ(req("-type_s:"+ parentType), "//*[@numFound='" + (holder.counter - parentsNum) + "']");
    
    // let's check BlockJoin
    Hierarchy randomHierarchy = holder.hierarchies.get(random().nextInt(holder.hierarchies.size()));
       
    Query deepestQuery = createBlockJoinQuery(randomHierarchy);
    assertSearch(deepestQuery, FIELD_ID, (String) randomHierarchy.elementData.get(FIELD_ID));
  }
  
  private Query createBlockJoinQuery(Hierarchy hierarchy) {
    List<Hierarchy> elements = hierarchy.elements;
    if (elements.isEmpty()) {
      BooleanQuery.Builder childQuery = new BooleanQuery.Builder();
      childQuery.add(new TermQuery(new Term(FIELD_ID, (String) hierarchy.elementData.get(FIELD_ID))), Occur.MUST);
      return childQuery.build();
    }
    
    Query childQuery = createBlockJoinQuery(elements.get(random().nextInt(elements.size())));
    return createToParentQuery(hierarchy.elementType, childQuery);
  }

  private ToParentBlockJoinQuery createToParentQuery(String parentType, String childField, String childFieldValue) {
    BooleanQuery.Builder childQuery = new BooleanQuery.Builder();
    childQuery.add(new TermQuery(new Term(childField, childFieldValue)), Occur.MUST);
    ToParentBlockJoinQuery result = createToParentQuery(parentType, childQuery.build());
    
    return result;
  }
  
  private ToParentBlockJoinQuery createToParentQuery(String parentType, Query childQuery) {
    ToParentBlockJoinQuery blockJoinQuery = new ToParentBlockJoinQuery(childQuery, createParentFilter(parentType), ScoreMode.Avg);
    
    return blockJoinQuery;
  }
  
  private void assertSearch(Query query, String field, String... values) throws IOException {
    /* The limit of search queue is doubled to catch the error in case when for some reason there are more docs than expected  */
    SolrIndexSearcher searcher = req.getSearcher();
    TopDocs result = searcher.search(query, values.length * 2);
    assertEquals(values.length, result.totalHits.value);
    List<String> actualValues = new ArrayList<String>();
    for (int index = 0; index < values.length; ++index) {
      Document doc = searcher.doc(result.scoreDocs[index].doc);
      actualValues.add(doc.get(field));
    }
    
    for (String expectedValue: values) {
      boolean removed = actualValues.remove(expectedValue);
      if (!removed) {
        fail("Search result does not contain expected values");
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  private List<String> createDataIterator(String query, String type, String description, int count) {
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    List<String> ids = new ArrayList<String>(count);
    for (int index = 0; index < count; ++index) {
      String docId = nextId();
      ids.add(docId);
      Map<String, Object> doc = createMap(FIELD_ID, docId, "desc", docId + " " + description, "type_s", type);
      data.add(doc);
    }
    Collections.shuffle(data, random());
    MockDataSource.setIterator(query, data.iterator());
    
    return ids;
  }
  
  /**
   * Creates randomized configuration of a specified depth. Simple configuration example:
   * 
   * <pre>
   * 
   * &lt;dataConfig>
   *   <dataSource type="MockDataSource" />
   *   &lt;document>
   *     &lt;entity name="parent" query="SELECT * FROM parent">
   *       &lt;field column="id" />
   *       &lt;field column="desc" />
   *       &lt;field column="type_s" />
   *       &lt;entity child="true" name="parentChild0" query="select * from parentChild0 where parentChild0_parent_id='${parent.id}'">
   *         &lt;field column="id" />
   *         &lt;field column="desc" />
   *         &lt;field column="type_s" />
   *         &lt;entity child="true" name="parentChild0Child0" query="select * from parentChild0Child0 where parentChild0Child0_parent_id='${parentChild0.id}'">
   *           &lt;field column="id" />
   *           &lt;field column="desc" />
   *           &lt;field column="type_s" />
   *         &lt;/entity>
   *         &lt;entity child="true" name="parentChild0Child1" query="select * from parentChild0Child1 where parentChild0Child1_parent_id='${parentChild0.id}'">
   *           &lt;field column="id" />
   *           &lt;field column="desc" />
   *           &lt;field column="type_s" />
   *         &lt;/entity>
   *       &lt;/entity>
   *       &lt;entity child="true" name="parentChild1" query="select * from parentChild1 where parentChild1_parent_id='${parent.id}'">
   *         &lt;field column="id" />
   *         &lt;field column="desc" />
   *         &lt;field column="type_s" />
   *         &lt;entity child="true" name="parentChild1Child0" query="select * from parentChild1Child0 where parentChild1Child0_parent_id='${parentChild1.id}'">
   *           &lt;field column="id" />
   *           &lt;field column="desc" />
   *           &lt;field column="type_s" />
   *         &lt;/entity>
   *         &lt;entity child="true" name="parentChild1Child1" query="select * from parentChild1Child1 where parentChild1Child1_parent_id='${parentChild1.id}'">
   *           &lt;field column="id" />
   *           &lt;field column="desc" />
   *           &lt;field column="type_s" />
   *         &lt;/entity>
   *       &lt;/entity>
   *     &lt;/entity>
   *   &lt;/document>
   * &lt;/dataConfig>
   * 
   * </pre>
   * 
   * Internally configures MockDataSource.
   **/
  private String createRandomizedConfig(int depth, String parentType, int parentsNum, ContextHolder holder) {
    List<Hierarchy> parentData = createMockedIterator(parentType, "SELECT * FROM " + parentType, parentsNum, holder);
    
    holder.hierarchies = parentData;
    
    String children = createChildren(parentType, 0, depth, parentData, holder);
    
    String rootFields = createFieldsList(FIELD_ID, "desc", "type_s");
    String rootEntity = StrUtils.formatString(ROOT_ENTITY_TEMPLATE, parentType, "SELECT * FROM " + parentType, rootFields, children);

    String config = StrUtils.formatString(DATA_CONFIG_TEMPLATE, rootEntity);
    return config;
  }
  
  @SuppressWarnings("unchecked")
  private List<Hierarchy> createMockedIterator(String type, String query, int amount, ContextHolder holder) {
    List<Hierarchy> hierarchies = new ArrayList<Hierarchy>();
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    for (int index = 0; index < amount; ++index) {
      holder.counter++;      
      String idStr = String.valueOf(holder.counter);
      Map<String, Object> element = createMap(FIELD_ID, idStr, "desc", type + "_" + holder.counter, "type_s", type);
      data.add(element);
      
      Hierarchy hierarchy = new Hierarchy();
      hierarchy.elementType = type;
      hierarchy.elementData = element;
      hierarchies.add(hierarchy);
    }
    
    MockDataSource.setIterator(query, data.iterator());
    
    return hierarchies;
  }
  
  private List<Hierarchy> createMockedIterator(String type, List<Hierarchy> parentData, ContextHolder holder) {
    List<Hierarchy> result = new ArrayList<Hierarchy>();
    for (Hierarchy parentHierarchy: parentData) {
      Map<String, Object> data = parentHierarchy.elementData;
      String id = (String) data.get(FIELD_ID);
      String select = String.format(Locale.ROOT, "select * from %s where %s='%s'", type, type + "_parent_id", id);
      
      // Number of actual children documents
      int childrenNum = 1 + random().nextInt(3);
      List<Hierarchy> childHierarchies = createMockedIterator(type, select, childrenNum, holder);
      parentHierarchy.elements.addAll(childHierarchies);
      result.addAll(childHierarchies);
    }
    return result;
  }

  private String createChildren(String parentName, int currentLevel, int maxLevel,
      List<Hierarchy> parentData, ContextHolder holder) {
    
    if (currentLevel == maxLevel) { //recursion base
      return "";
    }
    
    // number of different children <b>types</b> of parent, i.e. parentChild0, parentChild1
    // @see #createMockedIterator for the actual number of each children type 
    int childrenNumber = 2 + random().nextInt(3);
    StringBuilder builder = new StringBuilder();
    for (int childIndex = 0; childIndex < childrenNumber; ++childIndex) {
      String childName = parentName + "Child" + childIndex;
      String fields = createFieldsList(FIELD_ID, "desc", "type_s");
      String select = String.format(Locale.ROOT, "select * from %s where %s='%s'", childName, childName + "_parent_id", "${" + parentName + ".id}");
      
      //for each child entity create several iterators
      List<Hierarchy> childData = createMockedIterator(childName, parentData, holder);
      
      String subChildren = createChildren(childName, currentLevel + 1, maxLevel, childData, holder);
      String child = StrUtils.formatString(CHILD_ENTITY_TEMPLATE, childName, select, fields, subChildren);
      builder.append(child);
      builder.append('\n');
    }
    
    return builder.toString();
  }
  
  private String createFieldsList(String... fields) {
    StringBuilder builder = new StringBuilder();
    for (String field: fields) {
      String text = String.format(Locale.ROOT, "<field column='%s' />", field);
      builder.append(text);
      builder.append('\n');
    }
    return builder.toString();
  }

  private static final String THREE_LEVEL_HIERARCHY_CONFIG = "<dataConfig>\n" +
      "  <dataSource type='MockDataSource' />\n" +
      "  <document>\n" +
      "    <entity name='PARENT' query='select * from PARENT'>\n" +
      "      <field column='id' />\n" +
      "      <field column='desc' />\n" +
      "      <field column='type_s' />\n" +
      "      <entity child='true' name='CHILD' query=\"select * from CHILD where parent_id='${PARENT.id}'\">\n" +
      "        <field column='id' />\n" +
      "        <field column='desc' />\n" +
      "        <field column='type_s' />\n" +
      "          <entity child='true' name='GRANDCHILD' query=\"select * from GRANDCHILD where parent_id='${CHILD.id}'\">\n" +
      "            <field column='id' />\n" +
      "            <field column='desc' />\n" +
      "            <field column='type_s' />\n" +
      "          </entity>\n" +
      "      </entity>\n" +
      "    </entity>\n" +
      "  </document>\n" +
      "</dataConfig>";
  
  /** {0} is rootEntity block **/
  private static final String DATA_CONFIG_TEMPLATE = "<dataConfig><dataSource type=\"MockDataSource\" />\n<document>\n {0}</document></dataConfig>";
  
  /** 
   * {0} - entityName, 
   * {1} - select query
   * {2} - fieldsList
   * {3} - childEntitiesList 
   **/
  private static final String ROOT_ENTITY_TEMPLATE = "<entity name=\"{0}\" query=\"{1}\">\n{2} {3}\n</entity>\n";
  
  /** 
   * {0} - entityName, 
   * {1} - select query
   * {2} - fieldsList
   * {3} - childEntitiesList 
   **/
  private static final String CHILD_ENTITY_TEMPLATE = "<entity " + ConfigNameConstants.CHILD + "=\"true\" name=\"{0}\" query=\"{1}\">\n {2} {3} </entity>\n";
  
  private BitSetProducer createParentFilter(String type) {
    BooleanQuery.Builder parentQuery = new BooleanQuery.Builder();
    parentQuery.add(new TermQuery(new Term("type_s", type)), Occur.MUST);
    return new QueryBitSetProducer(parentQuery.build());
  }
  
  private String nextId() {
    ++id;
    return String.valueOf(id);
  }
  
}

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

package org.apache.solr.cloud;

import static org.apache.lucene.util.LuceneTestCase.random;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.schema.FieldTypeDefinition;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest.AddField;
import org.apache.solr.client.solrj.request.schema.SchemaRequest.AddFieldType;
import org.apache.solr.client.solrj.request.schema.SchemaRequest.MultiUpdate;
import org.apache.solr.client.solrj.request.schema.SchemaRequest.Update;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.Group;
import org.apache.solr.client.solrj.response.GroupCommand;
import org.apache.solr.client.solrj.response.GroupResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

public class DocValuesNotIndexedTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  @Rule
  public TestRule solrTestRules = RuleChain.outerRule(new SystemPropertiesRestoreRule());

  static final String COLLECTION = "dv_coll";


  volatile static List<FieldProps> fieldsToTestSingle = null;
  volatile static List<FieldProps> fieldsToTestMulti = null;
  volatile static List<FieldProps> fieldsToTestGroupSortFirst = null;
  volatile static List<FieldProps> fieldsToTestGroupSortLast = null;

  @BeforeClass
  public static void createCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    configureCluster(2)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-managed").resolve("conf"))
        .configure();

    // Need enough shards that we have some shards that don't have any docs on them.
    CollectionAdminRequest.createCollection(COLLECTION, "conf1", 4, 1)
        .setMaxShardsPerNode(2)
        .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(COLLECTION, 4, 4);

    fieldsToTestSingle =
        Collections.unmodifiableList(Arrays.asList(
            new FieldProps("intField", "int", 1),
            new FieldProps("longField", "long", 1),
            new FieldProps("doubleField", "double", 1),
            new FieldProps("floatField", "float", 1),
            new FieldProps("dateField", "date", 1),
            new FieldProps("stringField", "string", 1),
            new FieldProps("boolField", "boolean", 1),
            new FieldProps("sortableText", "sortabletext", 1)
        ));

    fieldsToTestMulti =
        Collections.unmodifiableList(Arrays.asList(
            new FieldProps("intFieldMulti", "int", 5),
            new FieldProps("longFieldMulti", "long", 5),
            new FieldProps("doubleFieldMulti", "double", 5),
            new FieldProps("floatFieldMulti", "float", 5),
            new FieldProps("dateFieldMulti", "date", 5),
            new FieldProps("stringFieldMulti", "string", 5),
            new FieldProps("boolFieldMulti", "boolean", 2),
            new FieldProps("sortableFieldMulti", "sortabletext", 5)
        ));

    // Fields to test for grouping and sorting with sortMissingFirst/Last.
    fieldsToTestGroupSortFirst =
        Collections.unmodifiableList(Arrays.asList(
            new FieldProps("intGSF", "int"),
            new FieldProps("longGSF", "long"),
            new FieldProps("doubleGSF", "double"),
            new FieldProps("floatGSF", "float"),
            new FieldProps("dateGSF", "date"),
            new FieldProps("stringGSF", "string"),
            new FieldProps("boolGSF", "boolean"),
            new FieldProps("sortableGSF", "sortabletext")
        ));

    fieldsToTestGroupSortLast =
        Collections.unmodifiableList(Arrays.asList(
            new FieldProps("intGSL", "int"),
            new FieldProps("longGSL", "long"),
            new FieldProps("doubleGSL", "double"),
            new FieldProps("floatGSL", "float"),
            new FieldProps("dateGSL", "date"),
            new FieldProps("stringGSL", "string"),
            new FieldProps("boolGSL", "boolean"),
            new FieldProps("sortableGSL", "sortabletext")
        ));

    List<Update> updateList = new ArrayList<>(fieldsToTestSingle.size() +
        fieldsToTestMulti.size() + fieldsToTestGroupSortFirst.size() + fieldsToTestGroupSortLast.size() +
        4);

    updateList.add(getType("name", "float", "class", RANDOMIZED_NUMERIC_FIELDTYPES.get(Float.class)));

    updateList.add(getType("name", "double", "class", RANDOMIZED_NUMERIC_FIELDTYPES.get(Double.class)));

    updateList.add(getType("name", "date", "class", RANDOMIZED_NUMERIC_FIELDTYPES.get(Date.class)));

    updateList.add(getType("name", "boolean", "class", "solr.BoolField"));


    // Add a field for each of the types we want to the schema.

    defineFields(updateList, fieldsToTestSingle, false);
    defineFields(updateList, fieldsToTestMulti, true);
    defineFields(updateList, fieldsToTestGroupSortFirst, false, "sorMissingFirst", "true");
    defineFields(updateList, fieldsToTestGroupSortLast, false, "sorMissingLast", "true");


    MultiUpdate multiUpdateRequest = new MultiUpdate(updateList);
    SchemaResponse.UpdateResponse multipleUpdatesResponse = multiUpdateRequest.process(cluster.getSolrClient(), COLLECTION);
    assertNull("Error adding fields", multipleUpdatesResponse.getResponse().get("errors"));

    cluster.getSolrClient().setDefaultCollection(COLLECTION);
  }


  @AfterClass
  public static void shutdown() throws Exception {
    shutdownCluster();
  }

  @Before
  public void clean() throws IOException, SolrServerException {
    CloudSolrClient client = cluster.getSolrClient();
    client.deleteByQuery("*:*");
    client.commit();
    resetFields(fieldsToTestSingle);
    resetFields(fieldsToTestMulti);
    resetFields(fieldsToTestGroupSortFirst);
    resetFields(fieldsToTestGroupSortLast);

  }

  void resetFields(List<FieldProps> fieldProps) {
    for (FieldProps prop : fieldProps) {
      prop.resetBase();
    }
  }
  @Test
  public void testDistribFaceting() throws IOException, SolrServerException {
    // For this test, I want to insure that there are shards that do _not_ have a doc with any of the DV_only 
    // fields, see SOLR-5260. So I'll add exactly 1 document to a 4 shard collection.

    CloudSolrClient client = cluster.getSolrClient();

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "1");
    for (FieldProps prop : fieldsToTestSingle) {
      doc.addField(prop.getName(), prop.getValue(true));
    }

    for (FieldProps prop : fieldsToTestMulti) {
      for (int idx = 0; idx < 5; ++idx) {
        doc.addField(prop.getName(), prop.getValue(true));
      }
    }

    new UpdateRequest()
        .add(doc)
        .commit(client, COLLECTION);

    final SolrQuery solrQuery = new SolrQuery("q", "*:*", "rows", "0");
    solrQuery.setFacet(true);
    for (FieldProps prop : fieldsToTestSingle) {
      solrQuery.addFacetField(prop.getName());
    }

    for (FieldProps prop : fieldsToTestMulti) {
      solrQuery.addFacetField(prop.getName());
    }

    final QueryResponse rsp = client.query(COLLECTION, solrQuery);

    for (FieldProps props : fieldsToTestSingle) {
      doTestFacet(props, rsp);
    }

    for (FieldProps props : fieldsToTestMulti) {
      doTestFacet(props, rsp);
    }
  }

  // We should be able to sort thing with missing first/last and that are _NOT_ present at all on one server.
  @Test
  public void testGroupingSorting() throws IOException, SolrServerException {
    CloudSolrClient client = cluster.getSolrClient();

    // The point of these is to have at least one shard w/o the value. 
    // While getting values for each of these fields starts _out_ random, each successive
    // _value_ increases.
    List<SolrInputDocument> docs = new ArrayList<>(3);
    docs.add(makeGSDoc(2, fieldsToTestGroupSortFirst, fieldsToTestGroupSortLast));
    docs.add(makeGSDoc(1, fieldsToTestGroupSortFirst, fieldsToTestGroupSortLast));
    docs.add(makeGSDoc(3, fieldsToTestGroupSortFirst, fieldsToTestGroupSortLast));
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", 4);
    docs.add(doc);

    new UpdateRequest()
        .add(docs)
        .commit(client, COLLECTION);

    checkSortOrder(client, fieldsToTestGroupSortFirst, "asc", new String[]{"4", "2", "1", "3"}, new String[]{"4", "1", "2", "3"});
    checkSortOrder(client, fieldsToTestGroupSortFirst, "desc", new String[]{"3", "1", "2", "4"}, new String[]{"2", "3", "1", "4"});

    checkSortOrder(client, fieldsToTestGroupSortLast, "asc", new String[]{"4", "2", "1", "3"}, new String[]{"4", "1", "2", "3"});
    checkSortOrder(client, fieldsToTestGroupSortLast, "desc", new String[]{"3", "1", "2", "4"}, new String[]{"2", "3", "1", "4"});

  }

  private void checkSortOrder(CloudSolrClient client, List<FieldProps> props, String sortDir, String[] order, String[] orderBool) throws IOException, SolrServerException {
    for (FieldProps prop : props) {
      final SolrQuery solrQuery = new SolrQuery("q", "*:*", "rows", "100");
      solrQuery.setSort(prop.getName(), "asc".equals(sortDir) ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc);
      solrQuery.addSort("id", SolrQuery.ORDER.asc);
      final QueryResponse rsp = client.query(COLLECTION, solrQuery);
      SolrDocumentList res = rsp.getResults();
      assertEquals("Should have exactly " + order.length + " documents returned", order.length, res.getNumFound());
      String expected;
      for (int idx = 0; idx < res.size(); ++idx) {
        if (prop.getName().startsWith("bool")) expected = orderBool[idx];
        else expected = order[idx];
        assertEquals("Documents in wrong order for field: " + prop.getName(),
            expected, res.get(idx).get("id"));
      }
    }
  }

  @Test
  public void testGroupingDocAbsent() throws IOException, SolrServerException {
    List<SolrInputDocument> docs = new ArrayList<>(4);
    docs.add(makeGSDoc(2, fieldsToTestGroupSortFirst, null));
    docs.add(makeGSDoc(1, fieldsToTestGroupSortFirst, null));
    docs.add(makeGSDoc(3, fieldsToTestGroupSortFirst, null));
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", 4);
    docs.add(doc);
    CloudSolrClient client = cluster.getSolrClient();

    new UpdateRequest()
        .add(docs)
        .commit(client, COLLECTION);

    // when grouping on any of these DV-only (not indexed) fields we expect exactly 4 groups except for Boolean.
    for (FieldProps prop : fieldsToTestGroupSortFirst) {
      // Special handling until SOLR-9802 is fixed
      if (prop.getName().startsWith("date")) continue;
      // SOLR-9802 to here

      final SolrQuery solrQuery = new SolrQuery("q", "*:*",
          "group", "true",
          "group.field", prop.getName());

      final QueryResponse rsp = client.query(COLLECTION, solrQuery);

      GroupResponse groupResponse = rsp.getGroupResponse();
      List<GroupCommand> commands = groupResponse.getValues();
      GroupCommand fieldCommand = commands.get(0);
      int expected = 4;
      if (prop.getName().startsWith("bool")) expected = 3; //true, false and null

      List<Group> fieldCommandGroups = fieldCommand.getValues();
      assertEquals("Did not find the expected number of groups for field " + prop.getName(), expected, fieldCommandGroups.size());
    }
  }

  @Test
  // Verify that we actually form groups that are "expected". Most of the processing takes some care to 
  // make sure all the values for each field are unique. We need to have docs that have values that are _not_
  // unique.
  public void testGroupingDVOnlySortFirst() throws IOException, SolrServerException {
    doGroupingDvOnly(fieldsToTestGroupSortFirst, "boolGSF");
  }

  @Test
  public void testGroupingDVOnlySortLast() throws IOException, SolrServerException {
    doGroupingDvOnly(fieldsToTestGroupSortLast, "boolGSL");
  }

  private void doGroupingDvOnly(List<FieldProps> fieldProps, String boolName) throws IOException, SolrServerException {
    List<SolrInputDocument> docs = new ArrayList<>(50);
    for (int idx = 0; idx < 49; ++idx) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", idx);

      // Every 7th doc we bump a counter by some random amount
      for (FieldProps prop : fieldProps) {
        doc.addField(prop.getName(), prop.getValue((idx % 7) == 0));
      }
      docs.add(doc);

      // Every fifth time through we add a doc with no values in any of the "fields of interest", so there should be
      // 10 docs with nulls
      if ((idx % 5) == 0) {
        doc = new SolrInputDocument();
        doc.addField("id", idx + 10_000);
        docs.add(doc);
      }
    }

    CloudSolrClient client = cluster.getSolrClient();

    new UpdateRequest()
        .add(docs)
        .commit(client, COLLECTION);

    // OK, we should have one group with 10 entries for null, a group with 1 entry and 7 groups with 7
    for (FieldProps prop : fieldProps) {

      // Solr 9802
      if (prop.getName().startsWith("date")) continue;

      final SolrQuery solrQuery = new SolrQuery(
          "q", "*:*",
          "rows", "100",
          "group", "true",
          "group.field", prop.getName(),
          "group.limit", "100",
          "group.sort", "id asc");

      final QueryResponse rsp = client.query(COLLECTION, solrQuery);

      GroupResponse groupResponse = rsp.getGroupResponse();
      List<GroupCommand> commands = groupResponse.getValues();


      int nullCount = 0;
      int sevenCount = 0;
      int boolCount = 0;
      for (int idx = 0; idx < commands.size(); ++idx) {
        GroupCommand fieldCommand = commands.get(idx);
        for (Group grp : fieldCommand.getValues()) {
          switch (grp.getResult().size()) {
            case 7:
              ++sevenCount;
              assertNotNull("Every group with 7 entries should have a group value.", grp.getGroupValue());
              break;
            case 10:
              ++nullCount;
              assertNull("This should be the null group", grp.getGroupValue());
              break;
            case 25:
            case 24:
              ++boolCount;
              assertEquals("We should have more counts for boolean fields!", boolName, prop.getName());
              break;
            
            default:
              fail("Unexpected number of elements in the group for '" + prop.getName() + "' size: '" + grp.getResult().size()
                  + "' GroupValue: '" + grp.getGroupValue()
                  + "' rsp: " + rsp);
          }
        }
      }
      assertEquals("Should be exactly one group with 1 entry of 10 for null for field " + prop.getName(), 1, nullCount);
      if (prop.getName().startsWith("bool")) {
        assertEquals("Should be exactly 2 groups with non-null Boolean types " + prop.getName(), 2, boolCount);
        assertEquals("Should be no seven count groups for Boolean types " + prop.getName(), 0, sevenCount);
      } else {
        assertEquals("Should be exactly 7 groups with seven entries for field " + prop.getName(), 7, sevenCount);
        assertEquals("Should be no gropus with 24 or 25 entries for field " + prop.getName(), 0, boolCount);
      }
    }
  }

  private SolrInputDocument makeGSDoc(int id, List<FieldProps> p1, List<FieldProps> p2, String... args) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", id);
    for (FieldProps prop : p1) {
      doc.addField(prop.getName(), prop.getValue(true));
    }

    if (p2 != null) {
      for (FieldProps prop : p2) {
        doc.addField(prop.getName(), prop.getValue(true));
      }
    }

    for (int idx = 0; idx < args.length; idx += 2) {
      doc.addField(args[idx], args[idx + 1]);
    }
    return doc;
  }


  private static void defineFields(List<Update> updateList, List<FieldProps> props, boolean multi, String... extras) {
    for (FieldProps prop : props) {
      Map<String, Object> fieldAttributes = new LinkedHashMap<>();
      fieldAttributes.put("name", prop.getName());
      fieldAttributes.put("type", prop.getType());
      fieldAttributes.put("indexed", "false");
      fieldAttributes.put("multiValued", multi ? "true" : "false");
      fieldAttributes.put("docValues", "true");
      updateList.add(new AddField(fieldAttributes));
    }
  }

  private static AddFieldType getType(String... args) {

    FieldTypeDefinition ftd = new FieldTypeDefinition();
    Map<String, Object> ftas = new LinkedHashMap<>();
    for (int idx = 0; idx < args.length; idx += 2) {
      ftas.put(args[idx], args[idx + 1]);
    }
    ftd.setAttributes(ftas);

    return new SchemaRequest.AddFieldType(ftd);
  }


  private void doTestFacet(FieldProps props, QueryResponse rsp) {
    String name = props.getName();
    final List<FacetField.Count> counts = rsp.getFacetField(name).getValues();
    long expectedCount = props.getExpectedCount();
    long foundCount = getCount(counts);
    assertEquals("Field " + name + " should have a count of " + expectedCount, expectedCount, foundCount);

  }

  private long getCount(final List<FacetField.Count> counts) {
    return counts.stream().mapToLong(FacetField.Count::getCount).sum();
  }
}

class FieldProps {

  private final String name;
  private final String type;
  private final int expectedCount;
  private Object base;
  private int counter = 0;

  FieldProps(String name, String type, int expectedCount) {
    this.name = name;
    this.type = type;
    this.expectedCount = expectedCount;
    resetBase();
  }

  // There's a vague chance that counts will roll over, so let's insure we have some room to grow in a positive direction
  void resetBase() {
    if (name.startsWith("int")) {
      base = Math.abs(random().nextInt()) / 2;
    } else if (name.startsWith("long")) {
      base = Math.abs(random().nextLong()) / 2;
    } else if (name.startsWith("float")) {
      base = Math.abs(random().nextFloat()) / 2;
    } else if (name.startsWith("double")) {
      base = Math.abs(random().nextDouble()) / 2;
    } else if (name.startsWith("date")) {
      base = Math.abs(random().nextLong()) / 2;
    } else if (name.startsWith("bool")) {
      base = true; // Must start with a known value since bools only have a two values....
    } else if (name.startsWith("string") || name.startsWith("sortable")) {
      base = "base_string_" + random().nextInt(1_000_000) + "_";
    } else {
      throw new RuntimeException("Should have found a prefix for the field before now!");
    }
    counter = 0;
  }

  FieldProps(String name, String type) {
    this(name, type, -1);
  }

  String getName() {
    return name;
  }

  String getType() {
    return type;
  }

  int getExpectedCount() {
    return expectedCount;
  }

  public String getValue(boolean incrementCounter) {
    if (incrementCounter) {
      counter += random().nextInt(10_000) + 1; // Must add something because nextInt could return zero
    }
    if (name.startsWith("int")) {
      return Integer.toString((int) base + counter);
    }
    if (name.startsWith("long")) {
      return Long.toString((long) base + counter);
    }
    if (name.startsWith("float")) {
      return Float.toString((float) base + counter);
    }
    if (name.startsWith("double")) {
      return Double.toString((double) base + counter);
    }
    if (name.startsWith("date")) {
      return Instant.ofEpochMilli(985_847_645 + (long) base + counter).toString();
    }
    if (name.startsWith("bool")) {
      String ret = Boolean.toString((boolean) base);
      base = !((boolean) base);
      return ret;
    }
    if (name.startsWith("string") || name.startsWith("sortable")) {
      return String.format(Locale.ROOT, "%s_%08d", (String) base, counter);
    }
    throw new RuntimeException("Should have found a prefix for the field before now!");
  }
}


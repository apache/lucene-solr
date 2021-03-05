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

package org.apache.solr.response;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.lucene.index.FieldInfo;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.DatePointField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import static junit.framework.Assert.fail;
import static org.apache.lucene.util.LuceneTestCase.random;
import static org.apache.solr.search.SolrReturnFields.FIELD_SOURCES.ALL_FROM_STORED;
import static org.apache.solr.search.SolrReturnFields.FIELD_SOURCES.MIXED_SOURCES;
import static org.apache.solr.search.SolrReturnFields.FIELD_SOURCES.ALL_FROM_DV;

public class TestRetrieveFieldsOptimizer extends SolrTestCaseJ4 {

  @Rule
  public TestRule solrTestRules = RuleChain.outerRule(new SystemPropertiesRestoreRule());

  @BeforeClass
  public static void initManagedSchemaCore() throws Exception {
    // This testing approach means no schema file or per-test temp solr-home!
    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("managed.schema.resourceName", "schema-one-field-no-dynamic-field-unique-key.xml");
    System.setProperty("enable.update.log", "false");

    initCore("solrconfig-managed-schema.xml", "ignoredSchemaName");

    IndexSchema schema = h.getCore().getLatestSchema();
    setupAllFields();

    h.getCore().setLatestSchema(schema);
  }

  static String storedNotDvSv = "_s_ndv_sv";
  static String storedAndDvSv = "_s_dv_sv";
  static String notStoredDvSv = "_ns_dv_sv";

  static String storedNotDvMv = "_s_ndv_mv";
  static String storedAndDvMv = "_s_dv_mv";
  static String notStoredDvMv = "_ns_dv_mv";

  // Each doc needs a field I can use to identify it for value comparison
  static String idStoredNotDv = "id_s_ndv_sv";
  static String idNotStoredDv = "id_ns_dv_sv";

  static FieldTypeHolder typesHolder = new FieldTypeHolder();

  static FieldHolder fieldsHolder = new FieldHolder();
  static Map<String, Map<String, List<String>>> allFieldValuesInput = new HashMap<>();

  //TODO, how to generalize?

  @SuppressWarnings({"unchecked"})
  private static void setupAllFields() throws IOException {

    IndexSchema schema = h.getCore().getLatestSchema();

    // Add all the types before the fields.
    Map<String, Map<String, String>> fieldsToAdd = new HashMap<>();

    // We need our special id fields to find the docs later.
    typesHolder.addFieldType(schema, idNotStoredDv, RetrieveFieldType.TEST_TYPE.STRING);
    fieldsToAdd.put(idNotStoredDv, map("stored", "false", "docValues", "true", "multiValued", "false"));

    typesHolder.addFieldType(schema, idStoredNotDv, RetrieveFieldType.TEST_TYPE.STRING);
    fieldsToAdd.put(idStoredNotDv, map("stored", "true", "docValues", "false", "multiValued", "false"));

    for (RetrieveFieldType.TEST_TYPE type : RetrieveFieldType.solrClassMap.keySet()) {
      // We happen to be naming the fields and types identically.
      String myName = type.toString() + storedNotDvSv;
      typesHolder.addFieldType(schema, myName, type);
      fieldsToAdd.put(myName, map("stored", "true", "docValues", "false", "multiValued", "false"));

      myName = type.toString() + storedAndDvSv;
      typesHolder.addFieldType(schema, myName, type);
      fieldsToAdd.put(myName, map("stored", "true", "docValues", "true", "multiValued", "false"));

      myName = type.toString() + notStoredDvSv;
      typesHolder.addFieldType(schema, myName, type);
      fieldsToAdd.put(myName, map("stored", "false", "docValues", "true", "multiValued", "false"));

      myName = type.toString() + storedNotDvMv;
      typesHolder.addFieldType(schema, myName, type);
      fieldsToAdd.put(myName, map("stored", "true", "docValues", "false", "multiValued", "true"));

      myName = type.toString() + storedAndDvMv;
      typesHolder.addFieldType(schema, myName, type);
      fieldsToAdd.put(myName, map("stored", "true", "docValues", "true", "multiValued", "true"));

      myName = type.toString() + notStoredDvMv;
      typesHolder.addFieldType(schema, myName, type);
      fieldsToAdd.put(myName, map("stored", "false", "docValues", "true", "multiValued", "true"));
    }

    schema = typesHolder.addFieldTypes(schema);

    for (Map.Entry<String, Map<String, String>> ent : fieldsToAdd.entrySet()) {
      fieldsHolder.addField(schema, ent.getKey(), ent.getKey(), ent.getValue());
    }
    schema = fieldsHolder.addFields(schema);

    h.getCore().setLatestSchema(schema);

    // All that setup work and we're only going to add a very few docs!
    for (int idx = 0; idx < 10; ++idx) {
      addDocWithAllFields(idx);
    }
    assertU(commit());
    // Now we need to massage the expected values returned based on the docValues type 'cause it's weird.
    final RefCounted<SolrIndexSearcher> refCounted = h.getCore().getNewestSearcher(true);
    try {
      //static Map<String, Map<String, List<String>>>
      for (Map<String, List<String>> docFieldsEnt : allFieldValuesInput.values()) {
        for (Map.Entry<String, List<String>> oneField : docFieldsEnt.entrySet()) {
          RetrieveField field = fieldsHolder.getTestField(oneField.getKey());
          field.expectedValsAsStrings(refCounted.get().getSlowAtomicReader().getFieldInfos().fieldInfo(field.name),
              oneField.getValue());
        }
      }
    } finally {
      refCounted.decref();
    }
   }

  static void addDocWithAllFields(int idx) {

    // for each doc, add a doc with all the fields with values and store the expected return.
    Map<String, List<String>> fieldsExpectedVals = new HashMap<>();

    SolrInputDocument sdoc = new SolrInputDocument();
    String id = "str" + idx;
    sdoc.addField("str", id);
    sdoc.addField(idNotStoredDv, id);
    fieldsExpectedVals.put(idNotStoredDv, Collections.singletonList(id));
    sdoc.addField(idStoredNotDv, id);
    fieldsExpectedVals.put(idStoredNotDv, Collections.singletonList(id));

    for (RetrieveField field : fieldsHolder.fields.values()) {
      if (field.name.equals(idNotStoredDv) || field.name.equals(idStoredNotDv)) {
        continue;
      }
      List<String> valsAsStrings = field.getValsForField();
      for (String val : valsAsStrings) {
        sdoc.addField(field.schemaField.getName(), val);
      }
      fieldsExpectedVals.put(field.name, valsAsStrings);
    }

    allFieldValuesInput.put(id, fieldsExpectedVals);
    assertU(adoc(sdoc));
  }

  @Test
  public void testDocFetcher() throws Exception {

    int numThreads = random().nextInt(3) + 2;

    Thread threads[] = new Thread[numThreads];
    for (int idx = 0; idx < numThreads; idx++) {
      threads[idx] = new Thread() {
        @Override
        public void run() {
          try {
            checkFetchSources(ALL_FROM_DV);
            checkFetchSources(ALL_FROM_STORED);
            checkFetchSources(MIXED_SOURCES);
          } catch (Exception e) {
            fail("Failed with exception " + e.getMessage());
          }
        }
      };
      threads[idx].start();
    }
    for (int idx = 0; idx < numThreads; idx++) {
      threads[idx].join();
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void checkFetchSources(SolrReturnFields.FIELD_SOURCES source) throws Exception {
    String flAll = fieldsHolder.allFields.stream()
        .map(RetrieveField::getName) // This will call testField.getName()
        .collect(Collectors.joining(","));

    List<RetrieveField> toCheck = new ArrayList<>();
    String idField = idNotStoredDv + ",";
    switch (source) {
      case ALL_FROM_DV:
        toCheck = new ArrayList(fieldsHolder.dvNotStoredFields);
        break;
      case ALL_FROM_STORED:
        idField = idStoredNotDv + ",";
        toCheck = new ArrayList(fieldsHolder.storedNotDvFields);
        break;
      case MIXED_SOURCES:
        toCheck = new ArrayList(fieldsHolder.allFields);
        break;
      default:
        fail("Value passed to checkFetchSources unknown: " + source.toString());
    }

    // MultiValued fields are _always_ read from stored data.
    toCheck.removeAll(fieldsHolder.multiValuedFields);

    // At this point, toCheck should be only singleValued fields. Adding in even a single multiValued field should
    // read stuff from stored.
    String fl = idField + toCheck.stream()
        .map(RetrieveField::getName) // This will call testField.getName()
        .collect(Collectors.joining(","));

    // Even a single multiValued and stored field should cause stored fields to be visited.

    List<Integer> shuffled = Arrays.asList(0, 1, 2);
    Collections.shuffle(shuffled, random());
    for (int which : shuffled) {
      switch (which) {
        case 0:
          check(fl, source);
          break;

        case 1:
          check(flAll, MIXED_SOURCES);
          break;

        case 2:
          List<RetrieveField> toCheckPlusMv = new ArrayList<>(toCheck);
          toCheckPlusMv.add(fieldsHolder.storedMvFields.get(random().nextInt(fieldsHolder.storedMvFields.size())));

          String flWithMv = idField + toCheckPlusMv.stream()
              .map(RetrieveField::getName) // This will call testField.getName()
              .collect(Collectors.joining(","));
          if (source == ALL_FROM_STORED) {
            check(flWithMv, ALL_FROM_STORED);
          } else {
            check(flWithMv, MIXED_SOURCES);
          }
          break;
        default:
          fail("Your shuffling should be between 0 and 2, inclusive. It was: " + which);
      }
    }
  }

  // This checks a couple of things:
  // 1> we got all the values from the place we expected.
  // 2> all the values we expect are actually returned.
  //
  // NOTE: multiValued fields are _NOT_ fetched from docValues by design so we don't have to worry about set semantics
  //
  private void check(String flIn, SolrReturnFields.FIELD_SOURCES source) throws Exception {
    Set<String> setDedupe = new HashSet<>(Arrays.asList(flIn.split(",")));
    String fl = String.join(",", setDedupe);

    SolrCore core = h.getCore();

    SolrQueryRequest req = lrf.makeRequest("q", "*:*", CommonParams.FL, fl);
    SolrQueryResponse rsp = h.queryAndResponse("", req);

    BinaryQueryResponseWriter writer = (BinaryQueryResponseWriter) core.getQueryResponseWriter("javabin");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    writer.write(baos, req, rsp);

    // This is really the main point!
    assertEquals("We didn't get the values from the expected places! ",
        source, ((SolrReturnFields) rsp.returnFields).getFieldSources());

    @SuppressWarnings({"rawtypes"})
    NamedList res;
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      res = (NamedList) jbc.unmarshal(new ByteArrayInputStream(baos.toByteArray()));
    }
    SolrDocumentList docs = (SolrDocumentList) res.get("response");
    for (Object doc : docs) {
      SolrDocument sdoc = (SolrDocument) doc;
      // Check that every (and only) the fields in the fl param were fetched and the values are as expected.
      // Since each doc has the same fields, we don't need to find the special doc.
      String[] requestedFields = fl.split(",");
      assertEquals("Should have exactly as many fields as requested, ", sdoc.getFieldNames().size(), requestedFields.length);

      String id = (String) sdoc.get(idNotStoredDv);
      if (id == null) {
        id = (String) sdoc.get(idStoredNotDv);
      }
      Map<String, List<String>> expected = allFieldValuesInput.get(id);
      for (String field : requestedFields) {
        Object[] docVals = sdoc.getFieldValues(field).toArray();
        RetrieveField testField = fieldsHolder.getTestField(field);
        List<String> expectedVals = expected.get(field);
        assertEquals("Returned fields should have the expected number of entries", docVals.length, expectedVals.size());
        for (int idx = 0; idx < docVals.length; ++idx) {
          assertEquals("Values should be identical and exactly in order. ", expectedVals.get(idx), testField.getValAsString(docVals[idx]));
        }
      }

    }
    req.close();
  }
}

class FieldTypeHolder {

  Map<String, RetrieveFieldType> testTypes = new HashMap<>();

  void addFieldType(IndexSchema schema, String name, RetrieveFieldType.TEST_TYPE type) {
    testTypes.put(name, new RetrieveFieldType(schema, name, type));
  }

  IndexSchema addFieldTypes(IndexSchema schema) {
    List<FieldType> typesToAdd = new ArrayList<>();
    for (RetrieveFieldType testType : testTypes.values()) {
      typesToAdd.add(testType.getFieldType());
    }
    return schema.addFieldTypes(typesToAdd, false);
  }

  RetrieveFieldType getTestType(String name) {
    return testTypes.get(name);
  }
}

class RetrieveFieldType {
  final String name;
  final FieldType solrFieldType;
  final TEST_TYPE testType;
  final String solrTypeClass;

  static enum TEST_TYPE {
    TINT, TLONG, TFLOAT, TDOUBLE, TDATE,
    PINT, PLONG, PFLOAT, PDOUBLE, PDATE,
    STRING, BOOL
  }

  static final Map<TEST_TYPE, String> solrClassMap = Collections.unmodifiableMap(Stream.of(
      new SimpleEntry<>(TEST_TYPE.TINT, "solr.TrieIntField"),
      new SimpleEntry<>(TEST_TYPE.TLONG, "solr.TrieLongField"),
      new SimpleEntry<>(TEST_TYPE.TFLOAT, "solr.TrieFloatField"),
      new SimpleEntry<>(TEST_TYPE.TDOUBLE, "solr.TrieDoubleField"),
      new SimpleEntry<>(TEST_TYPE.TDATE, "solr.TrieDateField"),
      new SimpleEntry<>(TEST_TYPE.PINT, "solr.IntPointField"),
      new SimpleEntry<>(TEST_TYPE.PLONG, "solr.LongPointField"),
      new SimpleEntry<>(TEST_TYPE.PFLOAT, "solr.FloatPointField"),
      new SimpleEntry<>(TEST_TYPE.PDOUBLE, "solr.DoublePointField"),
      new SimpleEntry<>(TEST_TYPE.PDATE, "solr.DatePointField"),
      new SimpleEntry<>(TEST_TYPE.STRING, "solr.StrField"),
      new SimpleEntry<>(TEST_TYPE.BOOL, "solr.BoolField"))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

  RetrieveFieldType(IndexSchema schema, String name, TEST_TYPE type) {
    this.name = name;
    Map<String, String> opts = new HashMap<>();
    opts.put("name", name);
    this.solrTypeClass = solrClassMap.get(type);
    opts.put("class", solrTypeClass);
    solrFieldType = schema.newFieldType(name, solrTypeClass, opts);
    this.testType = type;
  }

  FieldType getFieldType() {
    return solrFieldType;
  }

  String getSolrTypeClass() {
    return solrTypeClass;
  }
}

class FieldHolder {

  Map<String, RetrieveField> fields = new HashMap<>();

  void addField(IndexSchema schema, String name, String type, Map<String, String> opts) {
    fields.put(name, new RetrieveField(schema, name, type, opts));
  }

  List<RetrieveField> dvNotStoredFields = new ArrayList<>();
  List<RetrieveField> storedNotDvFields = new ArrayList<>();
  List<RetrieveField> multiValuedFields = new ArrayList<>();
  List<RetrieveField> storedAndDvFields = new ArrayList<>();
  List<RetrieveField> storedMvFields = new ArrayList<>();
  List<RetrieveField> allFields = new ArrayList<>();

  IndexSchema addFields(IndexSchema schema) {

    List<SchemaField> fieldsToAdd = new ArrayList<>();

    for (RetrieveField field : fields.values()) {
      allFields.add(field);
      SchemaField schemaField = field.schemaField;
      fieldsToAdd.add(schemaField);
      if (schemaField.multiValued()) {
        multiValuedFields.add(field);
      }
      if (schemaField.hasDocValues() && schemaField.stored() == false) {
        dvNotStoredFields.add(field);
      }
      if (schemaField.hasDocValues() == false && schemaField.stored()) {
        storedNotDvFields.add(field);
      }
      if (schemaField.hasDocValues() && schemaField.stored()) {
        storedAndDvFields.add(field);
      }
      if (schemaField.stored() && schemaField.multiValued()) {
        storedMvFields.add(field);
      }
    }
    return schema.addFields(fieldsToAdd, Collections.emptyMap(), false);
  }

  RetrieveField getTestField(String field) {
    return fields.get(field);
  }
}

class RetrieveField {
  final String name;
  final String type;
  final SchemaField schemaField;
  final RetrieveFieldType testFieldType;

  RetrieveField(IndexSchema schema, String name, String type, Map<String, String> opts) {

    Map<String, String> fullOpts = new HashMap<>(opts);
    fullOpts.put("name", name);
    fullOpts.put("type", type);

    this.name = name;
    this.type = type;
    this.schemaField = schema.newField(name, type, opts);
    this.testFieldType = TestRetrieveFieldsOptimizer.typesHolder.getTestType(type);

  }

  String getValAsString(Object val) {

    FieldType fieldType = schemaField.getType();

    //Why do mutliValued date fields get here as Strings whereas single-valued fields are Dates?
    // Why do BoolFields sometimes get here as "F" or "T"?
    if (val instanceof String) {
      if (fieldType instanceof TrieDateField || fieldType instanceof DatePointField) {
        long lVal = Long.parseLong((String) val);
        return (new Date(lVal).toInstant().toString());
      }
      if (fieldType instanceof BoolField) {
        if (val.equals("F")) return "false";
        if (val.equals("T")) return "true";
      }
      return (String) val;
    }
    if (fieldType instanceof TrieDateField || fieldType instanceof DatePointField) {
      return ((Date) val).toInstant().toString();
    }

    return val.toString();
  }

  String getName() {
    return schemaField.getName();
  }

  static String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

  private String randString() {
    StringBuilder sb = new StringBuilder();
    sb.setLength(0);

    for (int idx = 0; idx < 10; ++idx) {
      sb.append(chars.charAt(random().nextInt(chars.length())));
    }
    return sb.toString();
  }

  private String randDate() {
    return new Date(Math.abs(random().nextLong()) % 3_000_000_000_000L).toInstant().toString();
  }

  List<String> getValsForField() {
    List<String> valsAsStrings = new ArrayList<>();
    switch (testFieldType.getSolrTypeClass()) {
      case "solr.TrieIntField":
      case "solr.TrieLongField":
      case "solr.IntPointField":
      case "solr.LongPointField":
        valsAsStrings.add(Integer.toString(random().nextInt(10_000)));
        if (schemaField.multiValued() == false) break;
        for (int idx = 0; idx < random().nextInt(5); ++idx) {
          valsAsStrings.add(Integer.toString(random().nextInt(10_000)));
        }
        break;

      case "solr.TrieFloatField":
      case "solr.TrieDoubleField":
      case "solr.FloatPointField":
      case "solr.DoublePointField":
        valsAsStrings.add(Float.toString(random().nextFloat()));
        if (schemaField.multiValued() == false) break;
        for (int idx = 0; idx < random().nextInt(5); ++idx) {
          valsAsStrings.add(Float.toString(random().nextFloat()));
        }
        break;

      case "solr.TrieDateField":
      case "solr.DatePointField":
        valsAsStrings.add(randDate());
        if (schemaField.multiValued() == false) break;
        for (int idx = 0; idx < random().nextInt(5); ++idx) {
          valsAsStrings.add(randDate());
        }
        break;

      case "solr.StrField":
        valsAsStrings.add(randString());
        if (schemaField.multiValued() == false) break;
        for (int idx = 0; idx < random().nextInt(5); ++idx) {
          valsAsStrings.add(randString());
        }
        break;

      case "solr.BoolField":
        valsAsStrings.add(Boolean.toString(random().nextBoolean()));
        if (schemaField.multiValued() == false) break;
        for (int idx = 0; idx < random().nextInt(5); ++idx) {
          valsAsStrings.add(Boolean.toString(random().nextBoolean()));
        }
        break;

      default:
        fail("Found no case for field " + name + " type " + type);
        break;
    }
    // There are tricky cases with multiValued fields that are sometimes fetched from docValues that obey set
    // semantics so be sure we include at least one duplicate in a multValued field sometimes
    if (random().nextBoolean() && valsAsStrings.size() > 1) {
      valsAsStrings.add(valsAsStrings.get(random().nextInt(valsAsStrings.size())));

    }

    return valsAsStrings;
  }

  void expectedValsAsStrings(final FieldInfo info, List<String> valsAsStrings) {
    if (schemaField.stored() || schemaField.multiValued() == false) {
      return ;
    }

    switch (info.getDocValuesType()) {
      case NONE: // These three types are single values, just return.
      case NUMERIC:
      case BINARY: // here for completeness, really doesn't make sense.
        return;

      case SORTED_NUMERIC: // Can have multiple, identical values. This was a surprise to me.
        break;

      case SORTED_SET: // Obey set semantics.
      case SORTED:
        Set<String> uniq = new TreeSet<>(valsAsStrings);
        valsAsStrings.clear();
        valsAsStrings.addAll(uniq);
        break;
    }

    // Now order them if string-based comparison isn't reasonable
    switch (testFieldType.getSolrTypeClass()) {
      case "solr.TrieIntField":
      case "solr.TrieLongField":

        Collections.sort(valsAsStrings, Comparator.comparingInt(Integer::parseInt));
        break;
      case "solr.IntPointField":
      case "solr.LongPointField":
        Collections.sort(valsAsStrings, Comparator.comparingLong(Long::parseLong));
        break;

      case "solr.TrieFloatField":
      case "solr.FloatPointField":
      case "solr.TrieDoubleField":
      case "solr.DoublePointField":
        Collections.sort(valsAsStrings, Comparator.comparingDouble(Double::parseDouble));
        break;

      case "solr.TrieDateField":
      case "solr.DatePointField":
      case "solr.StrField":
      case "solr.BoolField":
        Collections.sort(valsAsStrings);
        break;

      default:
        fail("Found no case for field " + name + " type " + type);
        break;
    }
  }
}


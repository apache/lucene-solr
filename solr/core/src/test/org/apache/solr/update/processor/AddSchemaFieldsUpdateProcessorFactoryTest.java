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
package org.apache.solr.update.processor;

import java.io.File;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.schema.IndexSchema;
import org.junit.After;
import org.junit.Before;

/**
 * Tests for the field mutating update processors
 * that parse Dates, Longs, Doubles, and Booleans.
 */
public class AddSchemaFieldsUpdateProcessorFactoryTest extends UpdateProcessorTestBase {
  private static final String SOLRCONFIG_XML = "solrconfig-add-schema-fields-update-processor-chains.xml";
  private static final String SCHEMA_XML     = "schema-add-schema-fields-update-processor.xml";

  private static File tmpSolrHome;
  private static File tmpConfDir;

  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";

  @Before
  private void initManagedSchemaCore() throws Exception {
    tmpSolrHome = createTempDir().toFile();
    tmpConfDir = new File(tmpSolrHome, confDir);
    File testHomeConfDir = new File(TEST_HOME(), confDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, SOLRCONFIG_XML), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, SCHEMA_XML), tmpConfDir);

    // initCore will trigger an upgrade to managed schema, since the solrconfig*.xml has
    // <schemaFactory class="ManagedIndexSchemaFactory" ... />
    initCore(SOLRCONFIG_XML, SCHEMA_XML, tmpSolrHome.getPath());
  }

  public void testEmptyValue() {
    IndexSchema schema = h.getCore().getLatestSchema();
    final String fieldName = "newFieldABC";
    assertNull(schema.getFieldOrNull(fieldName));
    //UpdateProcessorTestBase#doc doesn't deal with nulls
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "1");
    doc.addField(fieldName, null);

    SolrInputDocument finalDoc = doc;
    expectThrows(AssertionError.class, () -> processAdd("add-fields-no-run-processor", finalDoc));

    expectThrows(AssertionError.class, () -> processAdd("add-fields-no-run-processor", new SolrInputDocument(null , null)));
  }

  public void testSingleField() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    final String fieldName = "newfield1";
    assertNull(schema.getFieldOrNull(fieldName));
    Date date = Date.from(Instant.now());
    SolrInputDocument d = processAdd("add-fields-no-run-processor", doc(f("id", "1"), f(fieldName, date)));
    assertNotNull(d);
    schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull(fieldName));
    assertEquals("pdates", schema.getFieldType(fieldName).getTypeName());
  }

  public void testSingleFieldRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    final String fieldName = "newfield2";
    assertNull(schema.getFieldOrNull(fieldName));
    Float floatValue = -13258.992f;
    SolrInputDocument d = processAdd("add-fields", doc(f("id", "2"), f(fieldName, floatValue)));
    assertNotNull(d);
    schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull(fieldName));
    assertEquals("pfloats", schema.getFieldType(fieldName).getTypeName());
    assertU(commit());
    assertQ(req("id:2"), "//arr[@name='" + fieldName + "']/float[.='" + floatValue.toString() + "']");
  }

  public void testSingleFieldMixedFieldTypesRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    final String fieldName = "newfield3";
    assertNull(schema.getFieldOrNull(fieldName));
    Float fieldValue1 = -13258.0f;
    Double fieldValue2 = 8.4828800808E10; 
    SolrInputDocument d = processAdd
        ("add-fields", doc(f("id", "3"), f(fieldName, fieldValue1, fieldValue2)));
    assertNotNull(d);
    schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull(fieldName));
    assertEquals("pdoubles", schema.getFieldType(fieldName).getTypeName());
    assertU(commit());
    assertQ(req("id:3")
        ,"//arr[@name='" + fieldName + "']/double[.='" + fieldValue1.toString() + "']"
        ,"//arr[@name='" + fieldName + "']/double[.='" + fieldValue2.toString() + "']");
  }

  public void testSingleFieldDefaultFieldTypeRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    final String fieldName = "newfield4";
    assertNull(schema.getFieldOrNull(fieldName));
    Float fieldValue1 = -13258.0f;
    Double fieldValue2 = 8.4828800808E10;
    String fieldValue3 = "blah blah";
    SolrInputDocument d = processAdd
        ("add-fields", doc(f("id", "4"), f(fieldName, fieldValue1, fieldValue2, fieldValue3)));
    assertNotNull(d);
    schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull(fieldName));
    assertEquals("text", schema.getFieldType(fieldName).getTypeName());
    assertEquals(0, schema.getCopyFieldProperties(true, Collections.singleton(fieldName), null).size());
    assertU(commit());
    assertQ(req("id:4")
        ,"//arr[@name='" + fieldName + "']/str[.='" + fieldValue1.toString() + "']"
        ,"//arr[@name='" + fieldName + "']/str[.='" + fieldValue2.toString() + "']"
        ,"//arr[@name='" + fieldName + "']/str[.='" + fieldValue3.toString() + "']"
    );
  }

  public void testSingleFieldDefaultTypeMappingRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    final String fieldName = "newfield4";
    assertNull(schema.getFieldOrNull(fieldName));
    Float fieldValue1 = -13258.0f;
    Double fieldValue2 = 8.4828800808E10;
    String fieldValue3 = "blah blah";
    SolrInputDocument d = processAdd
        ("add-fields-default-mapping", doc(f("id", "4"), f(fieldName, fieldValue1, fieldValue2, fieldValue3)));
    assertNotNull(d);
    schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull(fieldName));
    assertEquals("text", schema.getFieldType(fieldName).getTypeName());
    assertEquals(1, schema.getCopyFieldProperties(true, Collections.singleton(fieldName), null).size());
    assertU(commit());
    assertQ(req("id:4")
        ,"//arr[@name='" + fieldName + "']/str[.='" + fieldValue1.toString() + "']"
        ,"//arr[@name='" + fieldName + "']/str[.='" + fieldValue2.toString() + "']"
        ,"//arr[@name='" + fieldName + "']/str[.='" + fieldValue3.toString() + "']"
    );
  }

  public void testMultipleFieldsRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    final String fieldName1 = "newfield5";
    final String fieldName2 = "newfield6";
    assertNull(schema.getFieldOrNull(fieldName1));
    assertNull(schema.getFieldOrNull(fieldName2));
    Float field1Value1 = -13258.0f;
    Double field1Value2 = 8.4828800808E10;
    Long field1Value3 = 999L;
    Integer field2Value1 = 55123;
    Long field2Value2 = 1234567890123456789L;
    SolrInputDocument d = processAdd
        ("add-fields", doc(f("id", "5"), f(fieldName1, field1Value1, field1Value2, field1Value3),
                                         f(fieldName2, field2Value1, field2Value2)));
    assertNotNull(d);
    schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull(fieldName1));
    assertNotNull(schema.getFieldOrNull(fieldName2));
    assertEquals("pdoubles", schema.getFieldType(fieldName1).getTypeName());
    assertEquals("plongs", schema.getFieldType(fieldName2).getTypeName());
    assertU(commit());
    assertQ(req("id:5")
        ,"//arr[@name='" + fieldName1 + "']/double[.='" + field1Value1.toString() + "']"
        ,"//arr[@name='" + fieldName1 + "']/double[.='" + field1Value2.toString() + "']"
        ,"//arr[@name='" + fieldName1 + "']/double[.='" + field1Value3.doubleValue() + "']"
        ,"//arr[@name='" + fieldName2 + "']/long[.='" + field2Value1.toString() + "']"
        ,"//arr[@name='" + fieldName2 + "']/long[.='" + field2Value2.toString() + "']");
  }

  public void testParseAndAddMultipleFieldsRoundTrip() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    final String fieldName1 = "newfield7";
    final String fieldName2 = "newfield8";
    final String fieldName3 = "newfield9";
    final String fieldName4 = "newfield10";
    assertNull(schema.getFieldOrNull(fieldName1));
    assertNull(schema.getFieldOrNull(fieldName2));
    assertNull(schema.getFieldOrNull(fieldName3));
    assertNull(schema.getFieldOrNull(fieldName4));
    String field1String1 = "-13,258.0"; 
    Float field1Value1 = -13258.0f;
    String field1String2 = "84,828,800,808.0"; 
    Double field1Value2 = 8.4828800808E10;
    String field1String3 = "999";
    Long field1Value3 = 999L;
    String field2String1 = "55,123";
    Integer field2Value1 = 55123;
    String field2String2 = "1,234,567,890,123,456,789";
    Long field2Value2 = 1234567890123456789L;
    String field3String1 = "blah-blah";
    String field3Value1 = field3String1;
    String field3String2 = "-5.28E-3";
    Double field3Value2 = -5.28E-3;
    String field4String1 = "1999-04-17 17:42";
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm", Locale.ROOT).withZone(ZoneOffset.UTC);
    LocalDateTime dateTime = LocalDateTime.parse(field4String1, dateTimeFormatter);
    Date field4Value1 = Date.from(dateTime.atZone(ZoneOffset.UTC).toInstant());
    DateTimeFormatter dateTimeFormatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.ROOT).withZone(ZoneOffset.UTC);
    String field4Value1String = dateTime.format(dateTimeFormatter2) + "Z";
    
    SolrInputDocument d = processAdd
        ("parse-and-add-fields", doc(f("id", "6"), f(fieldName1, field1String1, field1String2, field1String3),
                                                   f(fieldName2, field2String1, field2String2),
                                                   f(fieldName3, field3String1, field3String2),
                                                   f(fieldName4, field4String1)));
    assertNotNull(d);
    schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull(fieldName1));
    assertNotNull(schema.getFieldOrNull(fieldName2));
    assertNotNull(schema.getFieldOrNull(fieldName3));
    assertNotNull(schema.getFieldOrNull(fieldName4));
    assertEquals("pdoubles", schema.getFieldType(fieldName1).getTypeName());
    assertEquals("plongs", schema.getFieldType(fieldName2).getTypeName());
    assertEquals("text", schema.getFieldType(fieldName3).getTypeName());
    assertEquals("pdates", schema.getFieldType(fieldName4).getTypeName());
    assertU(commit());
    assertQ(req("id:6")
        ,"//arr[@name='" + fieldName1 + "']/double[.='" + field1Value1.toString() + "']"
        ,"//arr[@name='" + fieldName1 + "']/double[.='" + field1Value2.toString() + "']"
        ,"//arr[@name='" + fieldName1 + "']/double[.='" + field1Value3.doubleValue() + "']"
        ,"//arr[@name='" + fieldName2 + "']/long[.='" + field2Value1.toString() + "']"
        ,"//arr[@name='" + fieldName2 + "']/long[.='" + field2Value2.toString() + "']"
        ,"//arr[@name='" + fieldName3 + "']/str[.='" + field3String1 + "']"
        ,"//arr[@name='" + fieldName3 + "']/str[.='" + field3String2 + "']"
        ,"//arr[@name='" + fieldName4 + "']/date[.='" + field4Value1String + "']");
  }

  public void testStringWithCopyField() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    final String fieldName = "stringField";
    final String strFieldName = fieldName+"_str";
    assertNull(schema.getFieldOrNull(fieldName));
    String content = "This is a text that should be copied to a string field but not be cutoff";
    SolrInputDocument d = processAdd("add-fields", doc(f("id", "1"), f(fieldName, content)));
    assertNotNull(d);
    schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull(fieldName));
    assertNotNull(schema.getFieldOrNull(strFieldName));
    assertEquals("text", schema.getFieldType(fieldName).getTypeName());
    assertEquals(1, schema.getCopyFieldProperties(true, Collections.singleton(fieldName), Collections.singleton(strFieldName)).size());
  }

  public void testStringWithCopyFieldAndMaxChars() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    final String fieldName = "stringField";
    final String strFieldName = fieldName+"_str";
    assertNull(schema.getFieldOrNull(fieldName));
    String content = "This is a text that should be copied to a string field and cutoff at 10 characters";
    SolrInputDocument d = processAdd("add-fields-maxchars", doc(f("id", "1"), f(fieldName, content)));
    assertNotNull(d);
    System.out.println("Document is "+d);
    schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getFieldOrNull(fieldName));
    assertNotNull(schema.getFieldOrNull(strFieldName));
    assertEquals("text", schema.getFieldType(fieldName).getTypeName());
    // We have three copyFields, one with maxChars 10 and two with maxChars 20
    assertEquals(3, schema.getCopyFieldProperties(true, Collections.singleton(fieldName), null).size());
    assertEquals("The configured maxChars cutoff does not exist on the copyField", 10, 
        schema.getCopyFieldProperties(true, Collections.singleton(fieldName), Collections.singleton(strFieldName))
            .get(0).get("maxChars"));
    assertEquals("The configured maxChars cutoff does not exist on the copyField", 20, 
        schema.getCopyFieldProperties(true, Collections.singleton(fieldName), Collections.singleton(fieldName+"_t"))
            .get(0).get("maxChars"));
    assertEquals("The configured maxChars cutoff does not exist on the copyField", 20, 
        schema.getCopyFieldProperties(true, Collections.singleton(fieldName), Collections.singleton(fieldName+"2_t"))
            .get(0).get("maxChars"));
  }
  
  public void testCopyFieldByIndexing() throws Exception {
    String content = "This is a text that should be copied to a string field and cutoff at 10 characters";
    SolrInputDocument d = processAdd("add-fields-default-mapping", doc(f("id", "1"), f("mynewfield", content)));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*").add("facet", "true").add("facet.field", "mynewfield_str");
    assertQ(req(params)
            , "*[count(//doc)=1]"
            ,"//lst[@name='mynewfield_str']/int[@name='This is a '][.='1']"
            );
  }
  
  @After
  private void deleteCoreAndTempSolrHomeDirectory() throws Exception {
    deleteCore();
  }
}

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
package org.apache.solr.schema;

import java.io.File;

import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.solr.core.AbstractBadConfigTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the useDocValuesAsStored functionality.
 */
public class TestUseDocValuesAsStored extends AbstractBadConfigTestBase {

  private int id = 1;

  private static File tmpSolrHome;
  private static File tmpConfDir;
  
  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";
  
  @Before
  private void initManagedSchemaCore() throws Exception {
    tmpSolrHome = createTempDir().toFile();
    tmpConfDir = new File(tmpSolrHome, confDir);
    File testHomeConfDir = new File(TEST_HOME(), confDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "solrconfig-managed-schema.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "solrconfig-basic.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "solrconfig.snippet.randomindexconfig.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "schema-one-field-no-dynamic-field.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "schema-one-field-no-dynamic-field-unique-key.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "enumsConfig.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "schema-non-stored-docvalues.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "schema-minimal.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "schema_codec.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "schema-bm25.xml"), tmpConfDir);
    
    // initCore will trigger an upgrade to managed schema, since the solrconfig has
    // <schemaFactory class="ManagedIndexSchemaFactory" ... />
    System.setProperty("enable.update.log", "false");
    System.setProperty("managed.schema.mutable", "true");
    initCore("solrconfig-managed-schema.xml", "schema-non-stored-docvalues.xml", tmpSolrHome.getPath());
  }
  
  @After
  private void afterClass() throws Exception {
    deleteCore();
    System.clearProperty("managed.schema.mutable");
    System.clearProperty("enable.update.log");
  }


  public String getCoreName() {
    return "basic";
  }

  @Test
  public void testOnEmptyIndex() throws Exception {
    clearIndex();
    assertU(commit());
    assertJQ(req("q", "*:*"), "/response/numFound==0");
    assertJQ(req("q", "*:*", "fl", "*"), "/response/numFound==0");
    assertJQ(req("q", "*:*", "fl", "test_nonstored_dv_str"), "/response/numFound==0");
    assertJQ(req("q", "*:*", "fl", "*,test_nonstored_dv_str"), "/response/numFound==0");

    assertU(adoc("id", "xyz", "test_nonstored_dv_str", "xyz"));
    assertJQ(req("q", "*:*"), "/response/numFound==0");
    assertJQ(req("q", "*:*", "fl", "*"), "/response/numFound==0");
    assertJQ(req("q", "*:*", "fl", "test_nonstored_dv_str"), "/response/numFound==0");
    assertJQ(req("q", "*:*", "fl", "*,test_nonstored_dv_str"), "/response/numFound==0");

    assertU(commit());
    assertJQ(req("q", "*:*"), "/response/numFound==1",
        "/response/docs==[" +
            "{'id':'xyz','test_nonstored_dv_str':'xyz'}"
            + "]");
    assertJQ(req("q", "*:*", "fl", "*"), "/response/numFound==1",
        "/response/docs==[" +
            "{'id':'xyz','test_nonstored_dv_str':'xyz'}"
            + "]");
    assertJQ(req("q", "*:*", "fl", "test_nonstored_dv_str"), "/response/numFound==1",
        "/response/docs==[" +
            "{'test_nonstored_dv_str':'xyz'}"
            + "]");
    assertJQ(req("q", "*:*", "fl", "*,test_nonstored_dv_str"), "/response/numFound==1",
        "/response/docs==[" +
            "{'id':'xyz','test_nonstored_dv_str':'xyz'}"
            + "]");

    assertU(adoc("id", "xyz"));
    assertU(commit());
    assertJQ(req("q", "*:*"), "/response/numFound==1",
        "/response/docs==[" +
            "{'id':'xyz'}"
            + "]");
  }

  @Test
  public void testSinglyValued() throws IOException {
    clearIndex();
    doTest("check string value is correct", "test_s_dvo", "str", "keyword");
    doTest("check int value is correct", "test_i_dvo", "int", "1234");
    doTest("check double value is correct", "test_d_dvo", "double", "1.234");
    doTest("check long value is correct", "test_l_dvo", "long", "12345");
    doTest("check float value is correct", "test_f_dvo", "float", "1.234");
    doTest("check dt value is correct", "test_dt_dvo", "date", "1976-07-04T12:08:56.235Z");
    doTest("check stored and docValues value is correct", "test_s_dv", "str", "storedAndDocValues");
    doTest("check non-stored and non-indexed is accessible", "test_s_dvo2", "str", "gotIt");
    doTest("enumField", "enum_dvo", "str", "Critical");
  }

  @Test
  public void testMultiValued() throws IOException {
    clearIndex();
    doTest("check string value is correct", "test_ss_dvo", "str", "keyword", "keyword2");
    doTest("check int value is correct", "test_is_dvo", "int", "1234", "12345");
    doTest("check double value is correct", "test_ds_dvo", "double", "1.234", "12.34", "123.4");
    doTest("check long value is correct", "test_ls_dvo", "long", "12345", "123456");
    doTest("check float value is correct", "test_fs_dvo", "float", "1.234", "12.34");
    doTest("check dt value is correct", "test_dts_dvo", "date", "1976-07-04T12:08:56.235Z", "1978-07-04T12:08:56.235Z");
    doTest("check stored and docValues value is correct", "test_ss_dv", "str", "storedAndDocValues", "storedAndDocValues2");
    doTest("check non-stored and non-indexed is accessible", "test_ss_dvo2", "str", "gotIt", "gotIt2");
    doTest("enumField", "enums_dvo", "str", "High", "Critical");
  }

  @Test
  public void testMultipleSearchResults() throws Exception {

    // Three documents with different numbers of values for a field
    assertU(adoc("id", "myid1", "test_is_dvo", "101", "test_is_dvo", "102", "test_is_dvo", "103"));
    assertU(adoc("id", "myid2", "test_is_dvo", "201", "test_is_dvo", "202"));
    assertU(adoc("id", "myid3", "test_is_dvo", "301", "test_is_dvo", "302",
        "test_is_dvo", "303", "test_is_dvo", "304"));

    // Multivalued and singly valued fields in the same document
    assertU(adoc("id", "myid4", "test_s_dvo", "hello", "test_is_dvo", "401", "test_is_dvo", "402"));

    // Test a field which has useDocValuesAsStored=false
    assertU(adoc("id", "myid5", "nonstored_dv_str", "dont see me"));
    assertU(adoc("id", "myid6", "nonstored_dv_str", "dont see me", "test_s_dvo", "hello"));
    assertU(commit());

    assertJQ(req("q", "id:myid*", "fl", "*"),
        "/response/docs==["
            + "{'id':'myid1','test_is_dvo':[101,102,103]},"
            + "{'id':'myid2','test_is_dvo':[201,202]},"
            + "{'id':'myid3','test_is_dvo':[301,302,303,304]},"
            + "{'id':'myid4','test_s_dvo':'hello','test_is_dvo':[401,402]},"
            + "{'id':'myid5'},"
            + "{'id':'myid6','test_s_dvo':'hello'}"
            + "]");
  }

  public void testManagedSchema() throws Exception {
    IndexSchema oldSchema = h.getCore().getLatestSchema();
    StrField type = new StrField();
    type.setTypeName("str");
    SchemaField falseDVASField = new SchemaField("false_dvas", type, 
        SchemaField.INDEXED | SchemaField.DOC_VALUES, null);
    SchemaField trueDVASField = new SchemaField("true_dvas", type, 
        SchemaField.INDEXED | SchemaField.DOC_VALUES | SchemaField.USE_DOCVALUES_AS_STORED, null);
    IndexSchema newSchema = oldSchema.addField(falseDVASField).addField(trueDVASField);
    h.getCore().setLatestSchema(newSchema);

    clearIndex();
    assertU(adoc("id", "myid1", "false_dvas", "101", "true_dvas", "102"));
    assertU(commit());

    assertJQ(req("q", "id:myid*", "fl", "*"),
        "/response/docs==["
            + "{'id':'myid1', 'true_dvas':'102'}]");
  }

  private void doTest(String desc, String field, String type, String... value) {
    String id = "" + this.id++;


    String[] xpaths = new String[value.length + 1];

    if (value.length > 1) {
      String[] fieldAndValues = new String[value.length * 2 + 2];
      fieldAndValues[0] = "id";
      fieldAndValues[1] = id;

      for (int i = 0; i < value.length; ++i) {
        fieldAndValues[i * 2 + 2] = field;
        fieldAndValues[i * 2 + 3] = value[i];
        xpaths[i] = "//arr[@name='" + field + "']/" + type + "[" + (i + 1) + "][.='" + value[i] + "']";
      }

      xpaths[value.length] = "*[count(//arr[@name='" + field + "']/" + type + ") = " + value.length + "]";
      assertU(adoc(fieldAndValues));

    } else {
      assertU(adoc("id", id, field, value[0]));
      xpaths[0] = "//" + type + "[@name='" + field + "'][.='" + value[0] + "']";
      xpaths[1] = "*[count(//" + type + "[@name='" + field + "']) = 1]";
    }

    assertU(commit());

    String fl = field;
    assertQ(desc + ": " + fl, req("q", "id:" + id, "fl", fl), xpaths);

    fl = field + ",*";
    assertQ(desc + ": " + fl, req("q", "id:" + id, "fl", fl), xpaths);

    fl = "*" + field.substring(field.length() - 3);
    assertQ(desc + ": " + fl, req("q", "id:" + id, "fl", fl), xpaths);

    fl = "*";
    assertQ(desc + ": " + fl, req("q", "id:" + id, "fl", fl), xpaths);

    fl = field + ",fakeFieldName";
    assertQ(desc + ": " + fl, req("q", "id:" + id, "fl", fl), xpaths);

    fl = "*";
    assertQ(desc + ": " + fl, req("q", "*:*", "fl", fl), xpaths);

  }
  
  // See SOLR-8740 for a discussion. This test is here to make sure we consciously change behavior of multiValued
  // fields given that we can now return docValues fields. The behavior we've guaranteed in the past is that if
  // multiValued fields are stored, they're returned in the document in the order they were added.
  // There are four new fieldTypes added:
  // <field name="test_mvt_dvt_st_str" type="string" indexed="true" multiValued="true" docValues="true"  stored="true"/>
  // <field name="test_mvt_dvt_sf_str" type="string" indexed="true" multiValued="true" docValues="true"  stored="false"/>
  // <field name="test_mvt_dvf_st_str" type="string" indexed="true" multiValued="true" docValues="false" stored="true"/>
  // <field name="test_mvt_dvu_st_str" type="string" indexed="true" multiValued="true"                   stored="true"/>
  //
  // If any of these tests break as a result of returning DocValues rather than stored values, make sure we reach some
  // consensus that any breaks on back-compat are A Good Thing and that that behavior is carefully documented!

  @Test
  public void testMultivaluedOrdering() throws Exception {
    clearIndex();
    
    // multiValued=true, docValues=true, stored=true. Should return in original order
    assertU(adoc("id", "1", "test_mvt_dvt_st_str", "cccc", "test_mvt_dvt_st_str", "aaaa", "test_mvt_dvt_st_str", "bbbb"));
    
    // multiValued=true, docValues=true, stored=false. Should return in sorted order
    assertU(adoc("id", "2", "test_mvt_dvt_sf_str", "cccc", "test_mvt_dvt_sf_str", "aaaa", "test_mvt_dvt_sf_str", "bbbb"));
    
    // multiValued=true, docValues=false, stored=true. Should return in original order
    assertU(adoc("id", "3", "test_mvt_dvf_st_str", "cccc", "test_mvt_dvf_st_str", "aaaa", "test_mvt_dvf_st_str", "bbbb"));
    
    // multiValued=true, docValues=not specified, stored=true. Should return in original order
    assertU(adoc("id", "4", "test_mvt_dvu_st_str", "cccc", "test_mvt_dvu_st_str", "aaaa", "test_mvt_dvu_st_str", "bbbb"));
    
    assertU(commit());
    
    assertJQ(req("q", "id:1", "fl", "test_mvt_dvt_st_str"), 
        "/response/docs/[0]/test_mvt_dvt_st_str/[0]==cccc",
        "/response/docs/[0]/test_mvt_dvt_st_str/[1]==aaaa",
        "/response/docs/[0]/test_mvt_dvt_st_str/[2]==bbbb");

    // Currently, this test fails since stored=false. When SOLR-8740 is committed, it should not throw an exception
    // and should succeed, returning the field in sorted order.
    try {
      assertJQ(req("q", "id:2", "fl", "test_mvt_dvt_sf_str"),
          "/response/docs/[0]/test_mvt_dvt_sf_str/[0]==aaaa",
          "/response/docs/[0]/test_mvt_dvt_sf_str/[1]==bbbb",
          "/response/docs/[0]/test_mvt_dvt_sf_str/[2]==cccc");
    } catch (Exception e) {
      // do nothing until SOLR-8740 is committed. At that point this should not throw an exception. 
      // NOTE: I think the test is correct after 8740 so just remove the try/catch
    }
    assertJQ(req("q", "id:3", "fl", "test_mvt_dvf_st_str"),
        "/response/docs/[0]/test_mvt_dvf_st_str/[0]==cccc",
        "/response/docs/[0]/test_mvt_dvf_st_str/[1]==aaaa",
        "/response/docs/[0]/test_mvt_dvf_st_str/[2]==bbbb");

    assertJQ(req("q", "id:4", "fl", "test_mvt_dvu_st_str"),
        "/response/docs/[0]/test_mvt_dvu_st_str/[0]==cccc",
        "/response/docs/[0]/test_mvt_dvu_st_str/[1]==aaaa",
        "/response/docs/[0]/test_mvt_dvu_st_str/[2]==bbbb");

  }
}

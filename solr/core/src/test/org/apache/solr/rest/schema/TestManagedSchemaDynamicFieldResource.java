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
package org.apache.solr.rest.schema;
import org.apache.commons.io.FileUtils;
import org.apache.solr.util.RestTestBase;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.restlet.ext.servlet.ServerServlet;

import java.io.File;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

public class TestManagedSchemaDynamicFieldResource extends RestTestBase {

  private static File tmpSolrHome;
  private static File tmpConfDir;

  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";


  @Before
  public void before() throws Exception {
    tmpSolrHome = createTempDir().toFile();
    tmpConfDir = new File(tmpSolrHome, confDir);
    FileUtils.copyDirectory(new File(TEST_HOME()), tmpSolrHome.getAbsoluteFile());

    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<>();
    final ServletHolder solrRestApi = new ServletHolder("SolrSchemaRestApi", ServerServlet.class);
    solrRestApi.setInitParameter("org.restlet.application", "org.apache.solr.rest.SolrSchemaRestApi");
    extraServlets.put(solrRestApi, "/schema/*");  // '/schema/*' matches '/schema', '/schema/', and '/schema/whatever...'

    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "false");

    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), "solrconfig-managed-schema.xml", "schema-rest.xml",
                          "/solr", true, extraServlets);
  }

  @After
  public void after() throws Exception {
    if (jetty != null) {
      jetty.stop();
      jetty = null;
    }
    client = null;
    if (restTestHarness != null) {
      restTestHarness.close();
    }
    restTestHarness = null;
  }

  @Test
  public void testAddDynamicFieldBadFieldType() throws Exception {
    assertJPut("/schema/dynamicfields/*_newdynamicfield",
               json( "{'type':'not_in_there_at_all','stored':false}" ),
               "/error/msg==\"Dynamic field \\'*_newdynamicfield\\': Field type \\'not_in_there_at_all\\' not found.\"");
  }

  @Test
  public void testAddDynamicFieldMismatchedName() throws Exception {
    assertJPut("/schema/dynamicfields/*_newdynamicfield",
               json( "{'name':'*_something_else','type':'text','stored':false}" ),
               "/error/msg=='///regex:\\\\*_newdynamicfield///'");
  }

  @Test
  public void testAddDynamicFieldBadProperty() throws Exception {
    assertJPut("/schema/dynamicfields/*_newdynamicfield",
               json( "{'type':'text','no_property_with_this_name':false}" ),
               "/error/msg==\"java.lang.IllegalArgumentException: Invalid field property: no_property_with_this_name\"");
  }

  @Test
  public void testAddDynamicField() throws Exception {
    assertQ("/schema/dynamicfields/newdynamicfield_*?indent=on&wt=xml",
            "count(/response/lst[@name='newdynamicfield_*']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");

    assertJPut("/schema/dynamicfields/newdynamicfield_*",
               json("{'type':'text','stored':false}"),
               "/responseHeader/status==0");

    assertQ("/schema/dynamicfields/newdynamicfield_*?indent=on&wt=xml",
            "count(/response/lst[@name='dynamicField']) = 1",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'");

    assertU(adoc("newdynamicfield_A", "value1 value2", "id", "123"));
    assertU(commit());

    assertQ("/select?q=newdynamicfield_A:value1",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
            "/response/result[@name='response'][@numFound='1']",
            "count(/response/result[@name='response']/doc/*) = 1",
            "/response/result[@name='response']/doc/str[@name='id'][.='123']");
  }

  @Test
  public void testAddDynamicFieldWithMulipleOptions() throws Exception {
    assertQ("/schema/dynamicfields/newdynamicfield_*?indent=on&wt=xml",
            "count(/response/lst[@name='dynamicField']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");

    assertJPut("/schema/dynamicfields/newdynamicfield_*",
               json("{'type':'text_en','stored':true,'indexed':false}"),
               "/responseHeader/status==0");

    File managedSchemaFile = new File(tmpConfDir, "managed-schema");
    assertTrue(managedSchemaFile.exists());
    String managedSchemaContents = FileUtils.readFileToString(managedSchemaFile, "UTF-8");
    Pattern newdynamicfieldStoredTrueIndexedFalsePattern
        = Pattern.compile( "<dynamicField name=\"newdynamicfield_\\*\" type=\"text_en\""
                         + "(?=.*stored=\"true\")(?=.*indexed=\"false\").*/>");
    assertTrue(newdynamicfieldStoredTrueIndexedFalsePattern.matcher(managedSchemaContents).find());

    assertQ("/schema/dynamicfields/newdynamicfield_*?indent=on&wt=xml",
            "count(/response/lst[@name='dynamicField']) = 1",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
            "/response/lst[@name='dynamicField']/str[@name='name'] = 'newdynamicfield_*'",
            "/response/lst[@name='dynamicField']/str[@name='type'] = 'text_en'",
            "/response/lst[@name='dynamicField']/bool[@name='indexed'] = 'false'",
            "/response/lst[@name='dynamicField']/bool[@name='stored'] = 'true'");

    assertU(adoc("newdynamicfield_A", "value1 value2", "id", "1234"));
    assertU(commit());

    assertQ("/schema/dynamicfields/newdynamicfield2_*?indent=on&wt=xml",
            "count(/response/lst[@name='dynamicField']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");

    assertJPut("/schema/dynamicfields/newdynamicfield2_*",
               json("{'type':'text_en','stored':true,'indexed':true,'multiValued':true}"),
               "/responseHeader/status==0");

    managedSchemaContents = FileUtils.readFileToString(managedSchemaFile, "UTF-8");
    Pattern newdynamicfield2StoredTrueIndexedTrueMultiValuedTruePattern
        = Pattern.compile( "<dynamicField name=\"newdynamicfield2_\\*\" type=\"text_en\" "
                         + "(?=.*stored=\"true\")(?=.*indexed=\"true\")(?=.*multiValued=\"true\").*/>");
    assertTrue(newdynamicfield2StoredTrueIndexedTrueMultiValuedTruePattern.matcher(managedSchemaContents).find());

    assertQ("/schema/dynamicfields/newdynamicfield2_*?indent=on&wt=xml",
            "count(/response/lst[@name='dynamicField']) = 1",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
            "/response/lst[@name='dynamicField']/str[@name='name'] = 'newdynamicfield2_*'",
            "/response/lst[@name='dynamicField']/str[@name='type'] = 'text_en'",
            "/response/lst[@name='dynamicField']/bool[@name='indexed'] = 'true'",
            "/response/lst[@name='dynamicField']/bool[@name='stored'] = 'true'",
            "/response/lst[@name='dynamicField']/bool[@name='multiValued'] = 'true'");

    assertU(adoc("newdynamicfield2_A", "value1 value2", "newdynamicfield2_A", "value3 value4", "id", "5678"));
    assertU(commit());

    assertQ("/select?q=newdynamicfield2_A:value3",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
            "/response/result[@name='response'][@numFound='1']",
            "count(/response/result[@name='response']/doc) = 1",
            "/response/result[@name='response']/doc/str[@name='id'][.='5678']");
  }

  @Test
  public void testAddDynamicFieldCollectionWithMultipleOptions() throws Exception {
    assertQ("/schema/dynamicfields?indent=on&wt=xml",
            "count(/response/arr[@name='dynamicFields']/lst/str[@name]) > 0",                          // there are fields
            "count(/response/arr[@name='dynamicFields']/lst/str[starts-with(@name,'newfield')]) = 0"); // but none named newfield*

    assertJPost("/schema/dynamicfields",
                json("[{'name':'newdynamicfield_*','type':'text_en','stored':true,'indexed':false}]"),
                "/responseHeader/status==0");

    File managedSchemaFile = new File(tmpConfDir, "managed-schema");
    assertTrue(managedSchemaFile.exists());
    String managedSchemaContents = FileUtils.readFileToString(managedSchemaFile, "UTF-8");
    Pattern newfieldStoredTrueIndexedFalsePattern
        = Pattern.compile( "<dynamicField name=\"newdynamicfield_\\*\" type=\"text_en\""
                         + "(?=.*stored=\"true\")(?=.*indexed=\"false\").*/>");
    assertTrue(newfieldStoredTrueIndexedFalsePattern.matcher(managedSchemaContents).find());

    assertQ("/schema/dynamicfields?indent=on&wt=xml",
             "/response/arr[@name='dynamicFields']/lst"
           + "[str[@name='name']='newdynamicfield_*' and str[@name='type']='text_en'"
           + " and bool[@name='stored']='true' and bool[@name='indexed']='false']");

    assertU(adoc("newdynamicfield_A", "value1 value2", "id", "789"));
    assertU(commit());

    assertJPost("/schema/dynamicfields",
                json("[{'name':'newdynamicfield2_*','type':'text_en','stored':true,'indexed':true,'multiValued':true}]"),
                "/responseHeader/status==0");

    managedSchemaContents = FileUtils.readFileToString(managedSchemaFile, "UTF-8");
    Pattern newdynamicfield2StoredTrueIndexedTrueMultiValuedTruePattern
        = Pattern.compile( "<dynamicField name=\"newdynamicfield2_\\*\" type=\"text_en\" "
                         + "(?=.*stored=\"true\")(?=.*indexed=\"true\")(?=.*multiValued=\"true\").*/>");
    assertTrue(newdynamicfield2StoredTrueIndexedTrueMultiValuedTruePattern.matcher(managedSchemaContents).find());

    assertQ("/schema/dynamicfields?indent=on&wt=xml",
             "/response/arr[@name='dynamicFields']/lst"
           + "[str[@name='name']='newdynamicfield2_*' and str[@name='type']='text_en'"
           + " and bool[@name='stored']='true' and bool[@name='indexed']='true' and bool[@name='multiValued']='true']");

    assertU(adoc("newdynamicfield2_A", "value1 value2", "newdynamicfield2_A", "value3 value4", "id", "790"));
    assertU(commit());

    assertQ("/select?q=newdynamicfield2_A:value3",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
            "/response/result[@name='response'][@numFound='1']",
            "count(/response/result[@name='response']/doc) = 1",
            "/response/result[@name='response']/doc/str[@name='id'][.='790']");
  }


  @Test
  public void testAddCopyField() throws Exception {
    assertQ("/schema/dynamicfields/newdynamicfield2_*?indent=on&wt=xml",
            "count(/response/lst[@name='dynamicField']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");

    assertJPut("/schema/dynamicfields/dynamicfieldA_*",
               json("{'type':'text','stored':false}"),
               "/responseHeader/status==0");
    assertJPut("/schema/dynamicfields/dynamicfieldB_*",
               json("{'type':'text','stored':false, 'copyFields':['dynamicfieldA_*']}"),
               "/responseHeader/status==0");
    assertJPut("/schema/dynamicfields/dynamicfieldC_*",
               json("{'type':'text','stored':false, 'copyFields':'dynamicfieldA_*'}"),
               "/responseHeader/status==0");

    assertQ("/schema/dynamicfields/dynamicfieldB_*?indent=on&wt=xml",
            "count(/response/lst[@name='dynamicField']) = 1",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'");
    assertQ("/schema/copyfields/?indent=on&wt=xml&source.fl=dynamicfieldB_*",
            "count(/response/arr[@name='copyFields']/lst) = 1");
    assertQ("/schema/copyfields/?indent=on&wt=xml&source.fl=dynamicfieldC_*",
            "count(/response/arr[@name='copyFields']/lst) = 1");
    //fine to pass in empty list, just won't do anything
    assertJPut("/schema/dynamicfields/dynamicfieldD_*",
               json("{'type':'text','stored':false, 'copyFields':[]}"),
               "/responseHeader/status==0");
    //some bad usages
    assertJPut("/schema/dynamicfields/dynamicfieldF_*",
               json("{'type':'text','stored':false, 'copyFields':['some_nonexistent_dynamicfield_ignore_exception_*']}"),
               "/error/msg==\"copyField dest :\\'some_nonexistent_dynamicfield_ignore_exception_*\\' is not an explicit field and doesn\\'t match a dynamicField.\"");
  }

  @Test
  public void testPostMultipleDynamicFields() throws Exception {
    assertQ("/schema/dynamicfields/newdynamicfield1_*?indent=on&wt=xml",
            "count(/response/lst[@name='dynamicField']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");

    assertQ("/schema/dynamicfields/newdynamicfield2_*?indent=on&wt=xml",
            "count(/response/lst[@name='dynamicField']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");

    assertJPost("/schema/dynamicfields",
                json( "[{'name':'newdynamicfield1_*','type':'text','stored':false},"
                    + " {'name':'newdynamicfield2_*','type':'text','stored':false}]"),
                "/responseHeader/status==0");

    assertQ("/schema/dynamicfields/newdynamicfield1_*?indent=on&wt=xml",
            "count(/response/lst[@name='dynamicField']) = 1",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'");

    assertQ("/schema/dynamicfields/newdynamicfield2_*?indent=on&wt=xml",
            "count(/response/lst[@name='dynamicField']) = 1",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'");

    assertU(adoc("newdynamicfield1_A", "value1 value2", "id", "123"));
    assertU(adoc("newdynamicfield2_A", "value3 value4", "id", "456"));
    assertU(commit());

    assertQ("/select?q=newdynamicfield1_A:value1",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
            "/response/result[@name='response'][@numFound='1']",
            "count(/response/result[@name='response']/doc/*) = 1",
            "/response/result[@name='response']/doc/str[@name='id'][.='123']");
    assertQ("/select?q=newdynamicfield2_A:value3",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
            "/response/result[@name='response'][@numFound='1']",
            "count(/response/result[@name='response']/doc/*) = 1",
            "/response/result[@name='response']/doc/str[@name='id'][.='456']");
  }

  @Test
  public void testPostCopy() throws Exception {
    assertJPost("/schema/dynamicfields",
                json( "[{'name':'dynamicfieldA_*','type':'text','stored':false},"
                    + " {'name':'dynamicfieldB_*','type':'text','stored':false},"
                    + " {'name':'dynamicfieldC_*','type':'text','stored':false, 'copyFields':['dynamicfieldB_*']}]"),
                "/responseHeader/status==0");
    assertQ("/schema/copyfields/?indent=on&wt=xml&source.fl=dynamicfieldC_*",
            "count(/response/arr[@name='copyFields']/lst) = 1");
    assertJPost("/schema/dynamicfields",
                json( "[{'name':'dynamicfieldD_*','type':'text','stored':false},"
                    + " {'name':'dynamicfieldE_*','type':'text','stored':false},"
                    + " {'name':'dynamicfieldF_*','type':'text','stored':false, 'copyFields':['dynamicfieldD_*','dynamicfieldE_*']},"
                    + " {'name':'dynamicfieldG_*','type':'text','stored':false, 'copyFields':'dynamicfieldD_*'}]"),//single
                "/responseHeader/status==0");
    assertQ("/schema/copyfields/?indent=on&wt=xml&source.fl=dynamicfieldF_*",
            "count(/response/arr[@name='copyFields']/lst) = 2");
    //passing in an empty list is perfectly acceptable, it just won't do anything
    assertJPost("/schema/dynamicfields",
                json( "[{'name':'dynamicfieldX_*','type':'text','stored':false},"
                    + " {'name':'dynamicfieldY_*','type':'text','stored':false},"
                    + " {'name':'dynamicfieldZ_*','type':'text','stored':false, 'copyFields':[]}]"),
                "/responseHeader/status==0");
    //some bad usages

    assertJPost("/schema/dynamicfields",
                json( "[{'name':'dynamicfieldH_*','type':'text','stored':false},"
                    + " {'name':'dynamicfieldI_*','type':'text','stored':false},"
                    + " {'name':'dynamicfieldJ_*','type':'text','stored':false, 'copyFields':['some_nonexistent_dynamicfield_ignore_exception_*']}]"),
                "/error/msg=='copyField dest :\\'some_nonexistent_dynamicfield_ignore_exception_*\\' is not an explicit field and doesn\\'t match a dynamicField.'");
  }

  @Test
  public void testPostCopyFields() throws Exception {
    assertJPost("/schema/dynamicfields",
                json( "[{'name':'dynamicfieldA_*','type':'text','stored':false},"
                    + " {'name':'dynamicfieldB_*','type':'text','stored':false},"
                    + " {'name':'dynamicfieldC_*','type':'text','stored':false},"
                    + " {'name':'dynamicfieldD_*','type':'text','stored':false},"
                    + " {'name':'dynamicfieldE_*','type':'text','stored':false}]"),
                "/responseHeader/status==0");
    assertJPost("/schema/copyfields",
                json( "[{'source':'dynamicfieldA_*', 'dest':'dynamicfieldB_*'},"
                    + " {'source':'dynamicfieldD_*', 'dest':['dynamicfieldC_*', 'dynamicfieldE_*']}]"),
                "/responseHeader/status==0");
    assertQ("/schema/copyfields/?indent=on&wt=xml&source.fl=dynamicfieldA_*",
            "count(/response/arr[@name='copyFields']/lst) = 1");
    assertQ("/schema/copyfields/?indent=on&wt=xml&source.fl=dynamicfieldD_*",
            "count(/response/arr[@name='copyFields']/lst) = 2");
    assertJPost("/schema/copyfields", // copyField glob sources are not required to match a dynamic field
                json("[{'source':'some_glob_not_necessarily_matching_any_dynamicfield_*', 'dest':['dynamicfieldA_*']},"
                    +" {'source':'*', 'dest':['dynamicfieldD_*']}]"),
                "/responseHeader/status==0");
    assertJPost("/schema/copyfields",
                json("[{'source':'dynamicfieldD_*', 'dest':['some_nonexistent_dynamicfield_ignore_exception_*']}]"),
                "/error/msg=='copyField dest :\\'some_nonexistent_dynamicfield_ignore_exception_*\\' is not an explicit field and doesn\\'t match a dynamicField.'");
  }
}


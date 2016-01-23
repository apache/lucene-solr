package org.apache.solr.rest.schema;
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

public class TestManagedSchemaFieldResource extends RestTestBase {

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
  public void testAddFieldBadFieldType() throws Exception {
    assertJPut("/schema/fields/newfield",
        json( "{'type':'not_in_there_at_all','stored':false}" ),
        "/error/msg==\"Field \\'newfield\\': Field type \\'not_in_there_at_all\\' not found.\"");
  }

  @Test
  public void testAddFieldMismatchedName() throws Exception {
    assertJPut("/schema/fields/newfield",
        json( "{'name':'something_else','type':'text','stored':false}" ),
        "/error/msg=='///regex:newfield///'");
  }
  
  @Test
  public void testAddFieldBadProperty() throws Exception {
    assertJPut("/schema/fields/newfield",
               json( "{'type':'text','no_property_with_this_name':false}" ),
               "/error/msg==\"java.lang.IllegalArgumentException: Invalid field property: no_property_with_this_name\"");
  }
  
  @Test
  public void testAddField() throws Exception {
    assertQ("/schema/fields/newfield?indent=on&wt=xml",
            "count(/response/lst[@name='field']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");
    
    assertJPut("/schema/fields/newfield",
               json("{'type':'text','stored':false}"),
               "/responseHeader/status==0");
    
    assertQ("/schema/fields/newfield?indent=on&wt=xml",
            "count(/response/lst[@name='field']) = 1",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'");
    
    assertU(adoc("newfield", "value1 value2", "id", "123"));
    assertU(commit());

    assertQ("/select?q=newfield:value1",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
            "/response/result[@name='response'][@numFound='1']",
            "count(/response/result[@name='response']/doc/*) = 1",
            "/response/result[@name='response']/doc/str[@name='id'][.='123']");
  }

  @Test
  public void testAddFieldWithMulipleOptions() throws Exception {
    assertQ("/schema/fields/newfield?indent=on&wt=xml",
            "count(/response/lst[@name='field']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");

    assertJPut("/schema/fields/newfield",
               json("{'type':'text_en','stored':true,'indexed':false}"),
               "/responseHeader/status==0");

    File managedSchemaFile = new File(tmpConfDir, "managed-schema");
    assertTrue(managedSchemaFile.exists());
    String managedSchemaContents = FileUtils.readFileToString(managedSchemaFile, "UTF-8");
    Pattern newfieldStoredTrueIndexedFalsePattern 
        = Pattern.compile( "<field name=\"newfield\" type=\"text_en\" "
                         + "(?=.*stored=\"true\")(?=.*indexed=\"false\").*/>");
    assertTrue(newfieldStoredTrueIndexedFalsePattern.matcher(managedSchemaContents).find());

    assertQ("/schema/fields/newfield?indent=on&wt=xml",
            "count(/response/lst[@name='field']) = 1",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
            "/response/lst[@name='field']/str[@name='name'] = 'newfield'",
            "/response/lst[@name='field']/str[@name='type'] = 'text_en'",
            "/response/lst[@name='field']/bool[@name='indexed'] = 'false'",
            "/response/lst[@name='field']/bool[@name='stored'] = 'true'");

    assertU(adoc("newfield", "value1 value2", "id", "1234"));
    assertU(commit());

    assertQ("/schema/fields/newfield2?indent=on&wt=xml",
            "count(/response/lst[@name='field']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");

    assertJPut("/schema/fields/newfield2",
               json("{'type':'text_en','stored':true,'indexed':true,'multiValued':true}"),
               "/responseHeader/status==0");

    managedSchemaContents = FileUtils.readFileToString(managedSchemaFile, "UTF-8");
    Pattern newfield2StoredTrueIndexedTrueMultiValuedTruePattern
        = Pattern.compile( "<field name=\"newfield2\" type=\"text_en\" "
                         + "(?=.*stored=\"true\")(?=.*indexed=\"true\")(?=.*multiValued=\"true\").*/>");
    assertTrue(newfield2StoredTrueIndexedTrueMultiValuedTruePattern.matcher(managedSchemaContents).find());

    assertQ("/schema/fields/newfield2?indent=on&wt=xml",
            "count(/response/lst[@name='field']) = 1",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
            "/response/lst[@name='field']/str[@name='name'] = 'newfield2'",
            "/response/lst[@name='field']/str[@name='type'] = 'text_en'",
            "/response/lst[@name='field']/bool[@name='indexed'] = 'true'",
            "/response/lst[@name='field']/bool[@name='stored'] = 'true'",
            "/response/lst[@name='field']/bool[@name='multiValued'] = 'true'");

    assertU(adoc("newfield2", "value1 value2", "newfield2", "value3 value4", "id", "5678"));
    assertU(commit());

    assertQ("/select?q=newfield2:value3",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
            "/response/result[@name='response'][@numFound='1']",
            "count(/response/result[@name='response']/doc) = 1",
            "/response/result[@name='response']/doc/str[@name='id'][.='5678']");
  }

  @Test
  public void testAddFieldCollectionWithMultipleOptions() throws Exception {
    assertQ("/schema/fields?indent=on&wt=xml",
            "count(/response/arr[@name='fields']/lst/str[@name]) > 0",                          // there are fields
            "count(/response/arr[@name='fields']/lst/str[starts-with(@name,'newfield')]) = 0"); // but none named newfield*

    assertJPost("/schema/fields",
               json("[{'name':'newfield','type':'text_en','stored':true,'indexed':false}]"),
               "/responseHeader/status==0");

    File managedSchemaFile = new File(tmpConfDir, "managed-schema");
    assertTrue(managedSchemaFile.exists());
    String managedSchemaContents = FileUtils.readFileToString(managedSchemaFile, "UTF-8");
    Pattern newfieldStoredTrueIndexedFalsePattern
        = Pattern.compile( "<field name=\"newfield\" type=\"text_en\" "
                         + "(?=.*stored=\"true\")(?=.*indexed=\"false\").*/>");
    assertTrue(newfieldStoredTrueIndexedFalsePattern.matcher(managedSchemaContents).find());

    assertQ("/schema/fields?indent=on&wt=xml",
             "/response/arr[@name='fields']/lst"
           + "[str[@name='name']='newfield' and str[@name='type']='text_en'"
           + " and bool[@name='stored']='true' and bool[@name='indexed']='false']");

    assertU(adoc("newfield", "value1 value2", "id", "789"));
    assertU(commit());

    assertJPost("/schema/fields",
                json("[{'name':'newfield2','type':'text_en','stored':true,'indexed':true,'multiValued':true}]"),
                "/responseHeader/status==0");

    managedSchemaContents = FileUtils.readFileToString(managedSchemaFile, "UTF-8");
    Pattern newfield2StoredTrueIndexedTrueMultiValuedTruePattern
        = Pattern.compile( "<field name=\"newfield2\" type=\"text_en\" "
                         + "(?=.*stored=\"true\")(?=.*indexed=\"true\")(?=.*multiValued=\"true\").*/>");
    assertTrue(newfield2StoredTrueIndexedTrueMultiValuedTruePattern.matcher(managedSchemaContents).find());

    assertQ("/schema/fields?indent=on&wt=xml",
            "/response/arr[@name='fields']/lst"
            + "[str[@name='name']='newfield2' and str[@name='type']='text_en'"
            + " and bool[@name='stored']='true' and bool[@name='indexed']='true' and bool[@name='multiValued']='true']");

    assertU(adoc("newfield2", "value1 value2", "newfield2", "value3 value4", "id", "790"));
    assertU(commit());

    assertQ("/select?q=newfield2:value3",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
            "/response/result[@name='response'][@numFound='1']",
            "count(/response/result[@name='response']/doc) = 1",
            "/response/result[@name='response']/doc/str[@name='id'][.='790']");
  }


  @Test
  public void testAddCopyField() throws Exception {
    assertQ("/schema/fields/newfield2?indent=on&wt=xml",
            "count(/response/lst[@name='field']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");

    assertJPut("/schema/fields/fieldA", 
               json("{'type':'text','stored':false}"),
               "/responseHeader/status==0");
    assertJPut("/schema/fields/fieldB",
               json("{'type':'text','stored':false, 'copyFields':['fieldA']}"),
               "/responseHeader/status==0");
    assertJPut("/schema/fields/fieldC",
               json("{'type':'text','stored':false, 'copyFields':'fieldA'}"),
               "/responseHeader/status==0");

    assertQ("/schema/fields/fieldB?indent=on&wt=xml",
            "count(/response/lst[@name='field']) = 1",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'");
    assertQ("/schema/copyfields/?indent=on&wt=xml&source.fl=fieldB",
        "count(/response/arr[@name='copyFields']/lst) = 1"
    );
    assertQ("/schema/copyfields/?indent=on&wt=xml&source.fl=fieldC",
        "count(/response/arr[@name='copyFields']/lst) = 1"
    );
    //fine to pass in empty list, just won't do anything
    assertJPut("/schema/fields/fieldD",
               json("{'type':'text','stored':false, 'copyFields':[]}"),
               "/responseHeader/status==0");
    //some bad usages
    assertJPut("/schema/fields/fieldF",
               json("{'type':'text','stored':false, 'copyFields':['some_nonexistent_field_ignore_exception']}"),
               "/error/msg==\"copyField dest :\\'some_nonexistent_field_ignore_exception\\' is not an explicit field and doesn\\'t match a dynamicField.\"");
  }

  @Test
  public void testPostMultipleFields() throws Exception {
    assertQ("/schema/fields/newfield1?indent=on&wt=xml",
            "count(/response/lst[@name='field']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");

    assertQ("/schema/fields/newfield2?indent=on&wt=xml",
            "count(/response/lst[@name='field']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");

    assertJPost("/schema/fields",
                json( "[{'name':'newfield1','type':'text','stored':false},"
                    + " {'name':'newfield2','type':'text','stored':false}]"),
                "/responseHeader/status==0");

    assertQ("/schema/fields/newfield1?indent=on&wt=xml",
            "count(/response/lst[@name='field']) = 1",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'");

    assertQ("/schema/fields/newfield2?indent=on&wt=xml",
            "count(/response/lst[@name='field']) = 1",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'");

    assertU(adoc("newfield1", "value1 value2", "id", "123"));
    assertU(adoc("newfield2", "value3 value4", "id", "456"));
    assertU(commit());

    assertQ("/select?q=newfield1:value1",
        "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
        "/response/result[@name='response'][@numFound='1']",
        "count(/response/result[@name='response']/doc/*) = 1",
        "/response/result[@name='response']/doc/str[@name='id'][.='123']");
    assertQ("/select?q=newfield2:value3",
        "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
        "/response/result[@name='response'][@numFound='1']",
        "count(/response/result[@name='response']/doc/*) = 1",
        "/response/result[@name='response']/doc/str[@name='id'][.='456']");
  }

  @Test
  public void testPostCopy() throws Exception {
    assertJPost("/schema/fields",
                json( "[{'name':'fieldA','type':'text','stored':false},"
                    + " {'name':'fieldB','type':'text','stored':false},"
                    + " {'name':'fieldC','type':'text','stored':false, 'copyFields':['fieldB']}]"),
                "/responseHeader/status==0");
    assertQ("/schema/copyfields/?indent=on&wt=xml&source.fl=fieldC",
        "count(/response/arr[@name='copyFields']/lst) = 1"
    );
    assertJPost("/schema/fields",
                json( "[{'name':'fieldD','type':'text','stored':false},"
                    + " {'name':'fieldE','type':'text','stored':false},"
                    + " {'name':'fieldF','type':'text','stored':false, 'copyFields':['fieldD','fieldE']},"
                    + " {'name':'fieldG','type':'text','stored':false, 'copyFields':'fieldD'}]"),//single
                "/responseHeader/status==0");
    assertQ("/schema/copyfields/?indent=on&wt=xml&source.fl=fieldF",
        "count(/response/arr[@name='copyFields']/lst) = 2"
    );
    //passing in an empty list is perfectly acceptable, it just won't do anything
    assertJPost("/schema/fields",
                json( "[{'name':'fieldX','type':'text','stored':false},"
                    + " {'name':'fieldY','type':'text','stored':false},"
                    + " {'name':'fieldZ','type':'text','stored':false, 'copyFields':[]}]"),
                "/responseHeader/status==0");
    //some bad usages

    assertJPost("/schema/fields",
                json( "[{'name':'fieldH','type':'text','stored':false},"
                    + " {'name':'fieldI','type':'text','stored':false},"
                    + " {'name':'fieldJ','type':'text','stored':false, 'copyFields':['some_nonexistent_field_ignore_exception']}]"),
                "/error/msg=='copyField dest :\\'some_nonexistent_field_ignore_exception\\' is not an explicit field and doesn\\'t match a dynamicField.'");
  }

  @Test
  public void testPostCopyFields() throws Exception {
    assertJPost("/schema/fields",
                json( "[{'name':'fieldA','type':'text','stored':false},"
                    + " {'name':'fieldB','type':'text','stored':false},"
                    + " {'name':'fieldC','type':'text','stored':false},"
                    + " {'name':'fieldD','type':'text','stored':false},"
                    + " {'name':'fieldE','type':'text','stored':false}]"),
                "/responseHeader/status==0");
    assertJPost("/schema/copyfields", 
                json( "[{'source':'fieldA', 'dest':'fieldB'},"
                    + " {'source':'fieldD', 'dest':['fieldC', 'fieldE']}]"),
                "/responseHeader/status==0");
    assertQ("/schema/copyfields/?indent=on&wt=xml&source.fl=fieldA",
        "count(/response/arr[@name='copyFields']/lst) = 1");
    assertQ("/schema/copyfields/?indent=on&wt=xml&source.fl=fieldD",
        "count(/response/arr[@name='copyFields']/lst) = 2");
    assertJPost("/schema/copyfields", 
                json("[{'source':'some_nonexistent_field_ignore_exception', 'dest':['fieldA']}]"),
                "/error/msg=='copyField source :\\'some_nonexistent_field_ignore_exception\\' is not a glob and doesn\\'t match any explicit field or dynamicField.'");
    assertJPost("/schema/copyfields", 
                json("[{'source':'fieldD', 'dest':['some_nonexistent_field_ignore_exception']}]"),
                "/error/msg=='copyField dest :\\'some_nonexistent_field_ignore_exception\\' is not an explicit field and doesn\\'t match a dynamicField.'");
  }
}


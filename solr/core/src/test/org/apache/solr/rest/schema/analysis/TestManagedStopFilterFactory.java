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
package org.apache.solr.rest.schema.analysis;

import java.io.File;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.RestTestBase;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the REST API for managing stop words, which is pretty basic:
 * GET: returns the list of stop words or a single word if it exists
 * PUT: add some words to the current list
 */
public class TestManagedStopFilterFactory extends RestTestBase {
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

    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "false");

    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), "solrconfig-managed-schema.xml", "schema-rest.xml",
                          "/solr", true, extraServlets);
  }

  @After
  private void after() throws Exception {
    if (null != jetty) {
      jetty.stop();
      jetty = null;
    }
    System.clearProperty("managed.schema.mutable");
    System.clearProperty("enable.update.log");
    
    if (restTestHarness != null) {
      restTestHarness.close();
    }
    restTestHarness = null;
  }


  /**
   * Test adding managed stopwords to an endpoint defined in the schema,
   * then adding docs containing a stopword before and after removing
   * the stopword from the managed stopwords set.
   */
  @Test
  public void testManagedStopwords() throws Exception {
    // invalid endpoint
    //// TODO: This returns HTML vs JSON because the exception is thrown
    ////       from the init method of ManagedEndpoint ... need a better solution
    // assertJQ("/schema/analysis/stopwords/bogus", "/error/code==404");
    
    // this endpoint depends on at least one field type containing the following
    // declaration in the schema-rest.xml:
    // 
    //   <filter class="solr.ManagedStopFilterFactory" managed="english" />
    //
    String endpoint = "/schema/analysis/stopwords/english";
    
    // test the initial GET request returns the default stopwords settings
    assertJQ(endpoint, 
             "/wordSet/initArgs/ignoreCase==false",
             "/wordSet/managedList==[]");
          
    // add some stopwords and verify they were added
    assertJPut(endpoint,
        Utils.toJSONString(Arrays.asList("a", "an", "the")),
               "/responseHeader/status==0");
          
    // test requesting a specific stop word that exists / does not exist
    assertJQ(endpoint + "/the", "/the=='the'");
    // not exist - 404
    assertJQ(endpoint + "/foo", "/error/code==404");
    // wrong case - 404
    assertJQ(endpoint + "/An", "/error/code==404");
    
    // update the ignoreCase initArg to true and make sure case is ignored
    String updateIgnoreCase = 
        "{ 'initArgs':{ 'ignoreCase':true }, "
        + "'managedList':['A','a','AN','an','THE','the','of','OF'] }";
    assertJPut(endpoint, json(updateIgnoreCase), "/responseHeader/status==0");
    
    assertJQ(endpoint, 
             "/wordSet/initArgs/ignoreCase==true",
             "/wordSet/managedList==['a','an','of','the']");
    
    // verify ignoreCase applies when requesting a word
    assertJQ("/schema/analysis/stopwords/english/The", "/The=='the'");

    // verify the resource supports XML writer type (wt) as well as JSON
    assertQ(endpoint,
            "count(/response/lst[@name='wordSet']/arr[@name='managedList']/*) = 4",
            "(/response/lst[@name='wordSet']/arr[@name='managedList']/str)[1] = 'a'",     
            "(/response/lst[@name='wordSet']/arr[@name='managedList']/str)[2] = 'an'",        
            "(/response/lst[@name='wordSet']/arr[@name='managedList']/str)[3] = 'of'",        
            "(/response/lst[@name='wordSet']/arr[@name='managedList']/str)[4] = 'the'");

    restTestHarness.reload();  // make the word set available

    String newFieldName = "managed_en_field";
    // make sure the new field doesn't already exist
    assertQ("/schema/fields/" + newFieldName + "?indent=on&wt=xml",
            "count(/response/lst[@name='field']) = 0",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '404'",
            "/response/lst[@name='error']/int[@name='code'] = '404'");

    // add the new field
    assertJPost("/schema/fields", "{add-field : { name :managed_en_field, type : managed_en}}",
               "/responseHeader/status==0");

    // make sure the new field exists now
    assertQ("/schema/fields/" + newFieldName + "?indent=on&wt=xml",
            "count(/response/lst[@name='field']) = 1",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'");

    assertU(adoc(newFieldName, "This is the one", "id", "6"));
    assertU(commit());

    assertQ("/select?q=" + newFieldName + ":This",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
            "/response/result[@name='response'][@numFound='1']",
            "/response/result[@name='response']/doc/str[@name='id'][.='6']");

    assertQ("/select?q=%7B%21raw%20f=" + newFieldName + "%7Dthe",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
            "/response/result[@name='response'][@numFound='0']");

    // verify delete works
    assertJDelete(endpoint + "/the", "/responseHeader/status==0");

    // verify that removing 'the' is not yet in effect
    assertU(adoc(newFieldName, "This is the other one", "id", "7"));
    assertU(commit());

    assertQ("/select?q=%7B%21raw%20f=" + newFieldName + "%7Dthe",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
            "/response/result[@name='response'][@numFound='0']");

    restTestHarness.reload();

    // verify that after reloading, removing 'the' has taken effect
    assertU(adoc(newFieldName, "This is the other other one", "id", "8"));
    assertU(commit());

    assertQ("/select?q=%7B%21raw%20f=" + newFieldName + "%7Dthe",
            "/response/lst[@name='responseHeader']/int[@name='status'] = '0'",
            "/response/result[@name='response'][@numFound='1']",
            "/response/result[@name='response']/doc/str[@name='id'][.='8']");

    assertJQ(endpoint,
             "/wordSet/initArgs/ignoreCase==true",
             "/wordSet/managedList==['a','an','of']");
    
    // should fail with 404 as foo doesn't exist
    assertJDelete(endpoint + "/foo", "/error/code==404");
  }

  /**
   * Can we add and remove stopwords with umlauts
   */
  @Test
  public void testCanHandleDecodingAndEncodingForStopwords() throws Exception  {
    String endpoint = "/schema/analysis/stopwords/german";

    //initially it should not exist
    assertJQ(endpoint + "/schön", "/error/code==404");

    //now we put a stopword with an umlaut
    assertJPut(endpoint,
        Utils.toJSONString(Arrays.asList("schön")),
        "/responseHeader/status==0");

    //let's check if it exists
    assertJQ(endpoint + "/schön", "/schön=='schön'");

    //now let's remove it
    assertJDelete(endpoint + "/schön", "/responseHeader/status==0");

    //and of it is unavailable again
    assertJQ(endpoint + "/schön", "/error/code==404");
  }
}

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
package org.apache.solr.handler.component;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Testing of pivot facets on multiple collections.
 * This class mainly aims to test that there is no issue with stop words on multiple collections.
 * Facets pivot counts are validated elsewhere.  There's no validation of counts here.
 */
@SuppressSSL
public class FacetPivot2CollectionsTest extends SolrCloudTestCase {
  private static final String COLL_A = "collectionA";
  private static final String COLL_B = "collectionB";
  private static final String ALIAS = "all";

  // available schema fields
  private static final String ID_FIELD = "id";
  private static final String TXT_FIELD_MULTIVALUED = "text";
  private static final String NAME_TXT_FIELD_NOT_MULTIVALUED = "name";
  private static final String TITLE_TXT_FIELD_NOT_MULTIVALUED = "title";
  private static final String SUBJECT_TXT_FIELD_MULTIVALUED = "subject";
  private static final String FILETYPE_TXT_FIELD_NOT_MULTIVALUED = "fileType";
  private static final String DYNAMIC_INT_FIELD_NOT_MULTIVALUED = "int_i";
  private static final String DYNAMIC_FLOAT_FIELD_NOT_MULTIVALUED = "float_f";
  private static final String DYNAMIC_DATE_FIELD_NOT_MULTIVALUED = "date_dt";
  private static final String DYNAMIC_STR_FIELD_MULTIVALUED = "strFieldMulti_s";
  private static final String DYNAMIC_STR_FIELD_NOT_MULTIVALUED = "strFieldSingle_s1";


  @BeforeClass
  public static void setupCluster() throws Exception {
    // create and configure cluster
    configureCluster(1)
        .addConfig(COLL_A, configset("different-stopwords" + File.separator + COLL_A))
        .addConfig(COLL_B, configset("different-stopwords" + File.separator + COLL_B))
        .configure();
    
    try {
      CollectionAdminResponse responseA = CollectionAdminRequest.createCollection(COLL_A,COLL_A,1,1).process(cluster.getSolrClient());
      NamedList<Object> result = responseA.getResponse();
      if(result.get("failure") != null) {
        fail("Collection A creation failed : " + result.get("failure"));
      }
    } catch (SolrException e) {
      fail("Collection A creation failed : " + e.getMessage());
    }

    try {
      CollectionAdminResponse responseB = CollectionAdminRequest.createCollection(COLL_B,COLL_B,1,1).process(cluster.getSolrClient());
      NamedList<Object> result = responseB.getResponse();
      if(result.get("failure") != null) {
        fail("Collection B creation failed : " + result.get("failure"));
      }
    }catch (SolrException e) {
      fail("Collection B creation failed : " + e.getMessage());    
    }
    
    CollectionAdminResponse response = CollectionAdminRequest.createAlias(ALIAS, COLL_A+","+COLL_B).process(cluster.getSolrClient());
    NamedList<Object> result = response.getResponse();
    if(result.get("failure") != null) {
      fail("Alias creation failed : " + result.get("failure"));
    }
    
    index(COLL_A, 10);
    index(COLL_B, 10);
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    shutdownCluster();
  }
  
  public void testOneCollectionPivotName() throws SolrServerException, IOException {
    SolrParams params = params("q", "*:*", "wt", "xml",
        "rows", "0",
        "facet", "true",
        "facet.field", SUBJECT_TXT_FIELD_MULTIVALUED,
        "facet.pivot", NAME_TXT_FIELD_NOT_MULTIVALUED);
    QueryResponse response = cluster.getSolrClient().query(COLL_A, params);
    NamedList<Object> result = response.getResponse();
    if(result.get("facet_counts") == null) {
      /*
       * Can happen if the random string that is used as facet pivot value is a stopword, or an empty string.
       * PivotFacetProcessor.getDocSet
       *  ft.getFieldQuery(null, field, "a") // a stopword
       *   -> returns null
       *  searcher.getDocSet(query, base);
       *   -> throws NPE
       *  
       *  ft.getFieldQuery(null, field, "") // empty str
       *   -> returned query= name:
       *  searcher.getDocSet(query, base);
       *   -> returns DocSet size -1
       */
      fail("Facet pivot on one collection failed");
    }
  }
  
  public void testOneCollectionPivotInt() throws SolrServerException, IOException {
    SolrParams params = params("q", "*:*", "wt", "xml",
        "rows", "0",
        "facet", "true",
        "facet.field", SUBJECT_TXT_FIELD_MULTIVALUED,
        "facet.pivot", DYNAMIC_INT_FIELD_NOT_MULTIVALUED);
    QueryResponse response = cluster.getSolrClient().query(COLL_A, params);
    NamedList<Object> result = response.getResponse();
    if(result.get("facet_counts") == null) {
      fail("Facet pivot on one collection failed");
    }
  }
  
  public void testOneCollectionPivotFloat() throws SolrServerException, IOException {
    SolrParams params = params("q", "*:*", "wt", "xml",
        "rows", "0",
        "facet", "true",
        "facet.field", SUBJECT_TXT_FIELD_MULTIVALUED,
        "facet.pivot", DYNAMIC_FLOAT_FIELD_NOT_MULTIVALUED);
    QueryResponse response = cluster.getSolrClient().query(COLL_A, params);
    NamedList<Object> result = response.getResponse();
    if(result.get("facet_counts") == null) {
      fail("Facet pivot on one collection failed");
    }
  }
  
  public void testOneCollectionPivotDate() throws SolrServerException, IOException {
    SolrParams params = params("q", "*:*", "wt", "xml",
        "rows", "0",
        "facet", "true",
        "facet.field", SUBJECT_TXT_FIELD_MULTIVALUED,
        "facet.pivot", DYNAMIC_DATE_FIELD_NOT_MULTIVALUED);
    QueryResponse response = cluster.getSolrClient().query(COLL_A, params);
    NamedList<Object> result = response.getResponse();
    if(result.get("facet_counts") == null) {
      fail("Facet pivot on one collection failed");
    }
  }
  
  public void testOneCollectionPivotTitleFileType() throws SolrServerException, IOException {
    SolrParams params = params("q", "*:*", "wt", "xml",
        "rows", "0",
        "facet", "true",
        "facet.field", TITLE_TXT_FIELD_NOT_MULTIVALUED,
        "facet.pivot", String.join(",", FILETYPE_TXT_FIELD_NOT_MULTIVALUED,TITLE_TXT_FIELD_NOT_MULTIVALUED));
    QueryResponse response = cluster.getSolrClient().query(COLL_A, params);
    NamedList<Object> result = response.getResponse();
    if(result.get("facet_counts") == null) {
      fail("Facet pivot on one collection failed");
    }
  }
  
  public void testAliasPivotName() throws SolrServerException, IOException {
    SolrParams params = params("q", "*:*", "wt", "xml",
        "rows", "0",
        "facet", "true",
        "facet.field", SUBJECT_TXT_FIELD_MULTIVALUED,
        "facet.pivot", NAME_TXT_FIELD_NOT_MULTIVALUED);
    final QueryResponse response = cluster.getSolrClient().query(ALIAS, params);
    NamedList<Object> result = response.getResponse();
    if(result.get("facet_counts") == null) {
      fail("Facet pivot on the alias failed");
    }
  }
  
  public void testAliasPivotType() throws SolrServerException, IOException {
    SolrParams params = params("q", "*:*", "wt", "xml",
        "rows", "0",
        "facet", "true",
        "facet.field", SUBJECT_TXT_FIELD_MULTIVALUED,
        "facet.pivot", FILETYPE_TXT_FIELD_NOT_MULTIVALUED);
    final QueryResponse response = cluster.getSolrClient().query(ALIAS, params);
    NamedList<Object> result = response.getResponse();
    if(result.get("facet_counts") == null) {
      fail("Facet pivot on the alias failed");
    }
  }
  
  public void testAliasPivotFloat() throws SolrServerException, IOException {
    SolrParams params = params("q", "*:*", "wt", "xml",
        "rows", "0",
        "facet", "true",
        "facet.field", SUBJECT_TXT_FIELD_MULTIVALUED,
        "facet.pivot", DYNAMIC_FLOAT_FIELD_NOT_MULTIVALUED);
    final QueryResponse response = cluster.getSolrClient().query(ALIAS, params);
    NamedList<Object> result = response.getResponse();
    if(result.get("facet_counts") == null) {
      fail("Facet pivot on the alias failed");
    }
  }
  
  public void testAliasPivotDate() throws SolrServerException, IOException {
    SolrParams params = params("q", "*:*", "wt", "xml",
        "rows", "0",
        "facet", "true",
        "facet.field", SUBJECT_TXT_FIELD_MULTIVALUED,
        "facet.pivot", DYNAMIC_FLOAT_FIELD_NOT_MULTIVALUED);
    final QueryResponse response = cluster.getSolrClient().query(ALIAS, params);
    NamedList<Object> result = response.getResponse();
    if(result.get("facet_counts") == null) {
      fail("Facet pivot on the alias failed");
    }
  }
  
  public void testAliasPivotTitleFileType() throws SolrServerException, IOException {
    SolrParams params = params("q", "*:*", "wt", "xml",
        "rows", "0",
        "facet", "true",
        "facet.field", TITLE_TXT_FIELD_NOT_MULTIVALUED,
        "facet.pivot", String.join(",", FILETYPE_TXT_FIELD_NOT_MULTIVALUED,TITLE_TXT_FIELD_NOT_MULTIVALUED));
    QueryResponse response = cluster.getSolrClient().query(ALIAS, params);
    NamedList<Object> result = response.getResponse();
    if(result.get("facet_counts") == null) {
      fail("Facet pivot on the alias failed");
    }
  }
  
  
  private static void index(final String collection, final int numDocs) throws SolrServerException, IOException {
    for(int i=0; i < numDocs; i++) {
      final Map<String,SolrInputField> fieldValues= addDocFields(i);
      final SolrInputDocument solrDoc = new SolrInputDocument(fieldValues);
      solrDoc.addField(DYNAMIC_DATE_FIELD_NOT_MULTIVALUED, skewed(randomSkewedDate(), randomDate()));
      solrDoc.addField(DYNAMIC_INT_FIELD_NOT_MULTIVALUED, skewed(TestUtil.nextInt(random(), 0, 100), random().nextInt()));
      solrDoc.addField(DYNAMIC_FLOAT_FIELD_NOT_MULTIVALUED, skewed(1.0F / random().nextInt(25), random().nextFloat() * random().nextInt()));
      cluster.getSolrClient().add(collection, solrDoc);
    }
    cluster.getSolrClient().commit(COLL_A);
    cluster.getSolrClient().commit(COLL_B);
  }

  private static Map<String,SolrInputField> addDocFields(final int id) {
    final Map<String,SolrInputField> fieldValues = new HashMap<>();
    final SolrInputField idField = new SolrInputField(ID_FIELD);
    idField.setValue(String.valueOf(id));
    fieldValues.put(ID_FIELD, idField);
    final SolrInputField textField1 = new SolrInputField(NAME_TXT_FIELD_NOT_MULTIVALUED);
    final SolrInputField textField2 = new SolrInputField(TXT_FIELD_MULTIVALUED);
    final SolrInputField textField3 = new SolrInputField(TITLE_TXT_FIELD_NOT_MULTIVALUED);
    final SolrInputField textField4 = new SolrInputField(FILETYPE_TXT_FIELD_NOT_MULTIVALUED);
    final SolrInputField textField5 = new SolrInputField(SUBJECT_TXT_FIELD_MULTIVALUED);
    final SolrInputField strField1 = new SolrInputField(DYNAMIC_STR_FIELD_NOT_MULTIVALUED);
    final SolrInputField strField2 = new SolrInputField(DYNAMIC_STR_FIELD_MULTIVALUED);
    final Random random = random();
    textField1.setValue(randomText(random, 10, true) + ".txt"); // make it look like a file name
    textField2.setValue(new String[] {randomText(random, 25, false), randomText(random, 25, false)});
    textField3.setValue(randomText(random, 10, true));
    textField4.setValue(randomText(random, 1, false));
    textField5.setValue(new String[] {randomText(random, 2, false), randomText(random, 5, false)});
    strField1.addValue(randomText(random, 1, false));
    strField2.addValue(new String[] {randomText(random, 1, false), randomText(random, 1, false)});
    fieldValues.put(NAME_TXT_FIELD_NOT_MULTIVALUED, textField1);
    fieldValues.put(TXT_FIELD_MULTIVALUED, textField2);
    if(random.nextInt(10) % 3 == 0) { // every now and then, a doc without a 'title' field.
      fieldValues.put(TITLE_TXT_FIELD_NOT_MULTIVALUED, textField3);
    }
    fieldValues.put(FILETYPE_TXT_FIELD_NOT_MULTIVALUED, textField4);
    fieldValues.put(SUBJECT_TXT_FIELD_MULTIVALUED, textField5);
    fieldValues.put(DYNAMIC_STR_FIELD_NOT_MULTIVALUED, strField1);
    fieldValues.put(DYNAMIC_STR_FIELD_MULTIVALUED, strField2);
    return fieldValues;
  }

  private static String randomText(final Random random, final int maxWords, final boolean addNonAlphaChars) {
    final StringBuilder builder = new StringBuilder();
    int words = random.nextInt(maxWords);
    while(words-- > 0) {
      String word = "";
      if(addNonAlphaChars && (words % 3 == 0)) {
        word = RandomStringUtils.random(random.nextInt(3), "\\p{Digit}\\p{Punct}");
        System.out.println("generated non-alpha string:" + word);     
      } else {
        word = RandomStringUtils.randomAlphabetic(1, 10);
      }
      builder.append(word).append(" ");
    }
    return builder.toString().trim();
  }
}

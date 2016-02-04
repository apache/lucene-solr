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

import java.util.Collection;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.junit.BeforeClass;
import org.junit.Test;
/**
 * 
 */
public class RequiredFieldsTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema-required-fields.xml");
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
  }
  
  @Test
  public void testRequiredFieldsConfig() {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getLatestSchema();
    SchemaField uniqueKey = schema.getUniqueKeyField();

    // Make sure the uniqueKey is required
    assertTrue( uniqueKey.isRequired() ); 
    assertTrue( schema.getRequiredFields().contains( uniqueKey ) );
    
    // we specified one required field, but all devault valued fields are also required
    Collection<SchemaField> requiredFields =schema.getRequiredFields();
    int numDefaultFields = schema.getFieldsWithDefaultValue().size();
    assertEquals( numDefaultFields+1+1, requiredFields.size()); // also the uniqueKey
  }
  
  @Test
  public void testRequiredFieldsSingleAdd() {      
    SolrCore core = h.getCore();     
    // Add a single document
    assertU("adding document",
      adoc("id", "529", "name", "document with id, name, and subject", "field_t", "what's inside?", "subject", "info"));
    assertU(commit());
    
    // Check it it is in the index
    assertQ("should find one", req("id:529") ,"//result[@numFound=1]" );

    // Add another document without the required subject field, which
    // has a configured defaultValue of "Stuff"
    assertU("adding a doc without field w/ configured default",
          adoc("id", "530", "name", "document with id and name", "field_t", "what's inside?"));
    assertU(commit());

    // Add another document without a subject, which has a default in schema
    String subjectDefault = core.getLatestSchema().getField("subject").getDefaultValue();
    assertNotNull("subject has no default value", subjectDefault);
    assertQ("should find one with subject="+subjectDefault, req("id:530 subject:"+subjectDefault) ,"//result[@numFound=1]" );

    // Add another document without a required name, which has no default
    assertNull(core.getLatestSchema().getField("name").getDefaultValue());
    ignoreException("missing required field");
    assertFailedU("adding doc without required field",
          adoc("id", "531", "subject", "no name document", "field_t", "what's inside?") );
    resetExceptionIgnores();
    assertU(commit());
    
    // Check to make sure this submission did not succeed
    assertQ("should not find any", req("id:531") ,"//result[@numFound=0]" ); 
  }
  
  @Test
  public void testAddMultipleDocumentsWithErrors() {
    //Add three documents at once to make sure the baseline succeeds
    assertU("adding 3 documents",
      "<add>" +doc("id", "601", "name", "multiad one", "field_t", "what's inside?", "subject", "info") +
      doc("id", "602", "name", "multiad two", "field_t", "what's inside?", "subject", "info") +
        doc("id", "603", "name", "multiad three", "field_t", "what's inside?", "subject", "info") +
        "</add>");
    assertU(commit());

    // Check that they are in the index
    assertQ("should find three", req("name:multiad") ,"//result[@numFound=3]" );
    
    // Add three documents at once, with the middle one missing a field that has a default
    assertU("adding 3 docs, with 2nd one missing a field that has a default value",
      "<add>" +doc("id", "601", "name", "nosubject batch one", "field_t", "what's inside?", "subject", "info") +
      doc("id", "602", "name", "nosubject batch two", "field_t", "what's inside?") +
        doc("id", "603", "name", "nosubject batch three", "field_t", "what's inside?", "subject", "info") +
        "</add>");
    assertU(commit());
    
    // Since the missing field had a devault value,
    // All three should have made it into the index
    assertQ("should find three", req("name:nosubject") ,"//result[@numFound=3]" );
    

    // Add three documents at once, with the middle with a bad field definition,
    // to establish the baselinie behavior for errors in a multi-ad submission
    assertFailedU("adding 3 documents, with 2nd one with undefined field",
          "<add>" +doc("id", "801", "name", "baddef batch one", "field_t", "what's inside?", "subject", "info") +
          doc("id", "802", "name", "baddef batch two", "missing_field_ignore_exception", "garbage") +
            doc("id", "803", "name", "baddef batch three", "field_t", "what's inside?", "subject", "info") +
            "</add>");
    assertU(commit());    

    // Check that only docs before the error should be in the index
    assertQ("should find one", req("name:baddef") ,"//result[@numFound=1]" );

    ignoreException("missing required field");
    // Add three documents at once, with the middle one missing a required field that has no default
    assertFailedU("adding 3 docs, with 2nd one missing required field",
      "<add>" +doc("id", "701", "name", "noname batch one", "field_t", "what's inside?", "subject", "info") +
      doc("id", "702", "field_t", "what's inside?", "subject", "info") +
        doc("id", "703", "name", "noname batch batch three", "field_t", "what's inside?", "subject", "info") +
        "</add>");
    resetExceptionIgnores();

    assertU(commit());

    // Check that only docs before the error should be in the index
    assertQ("should find one", req("name:noname") ,"//result[@numFound=1]" );
  }  
}

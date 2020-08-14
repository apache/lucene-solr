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

import java.lang.invoke.MethodHandles;

import org.apache.solr.request.LocalSolrQueryRequest;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test with various combinations of parameters, child entities, transformers.
 */
public class TestSqlEntityProcessorDelta extends AbstractSqlEntityProcessorTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private boolean delta = false;
  private boolean useParentDeltaQueryParam = false;
  private IntChanges personChanges = null;
  private String[] countryChanges = null;
  
  @Before
  public void setupDeltaTest() {
    delta = false;
    personChanges = null;
    countryChanges = null;
  }
  @Test
  public void testSingleEntity() throws Exception {
    log.debug("testSingleEntity full-import...");
    singleEntity(1);
    logPropertiesFile();
    changeStuff();
    int c = calculateDatabaseCalls();
    log.debug("testSingleEntity delta-import ({} database calls expected)...", c);
    singleEntity(c);
    validateChanges();
  }
  
  @Test
  public void testDeltaImportWithoutInitialFullImport() throws Exception {
    log.debug("testDeltaImportWithoutInitialFullImport delta-import...");
    countryEntity = false;
    delta = true;
    /*
     * We need to add 2 in total: 
     * +1 for deltaQuery i.e identifying id of items to update, 
     * +1 for deletedPkQuery i.e delete query
     */
    singleEntity(totalPeople() + 2);
    validateChanges();
  }

  @Test
  public void testWithSimpleTransformer() throws Exception {
    log.debug("testWithSimpleTransformer full-import...");    
    simpleTransform(1); 
    logPropertiesFile(); 
    changeStuff();
    int c = calculateDatabaseCalls();
    simpleTransform(c);
    log.debug("testWithSimpleTransformer delta-import ({} database calls expected)...", c);
    validateChanges(); 
  }
  @Test
  public void testWithComplexTransformer() throws Exception {
    log.debug("testWithComplexTransformer full-import...");     
    complexTransform(1, 0);
    logPropertiesFile();
    changeStuff();
    int c = calculateDatabaseCalls();
    log.debug("testWithComplexTransformer delta-import ({} database calls expected)...", c);
    complexTransform(c, personChanges.deletedKeys.length);
    validateChanges();  
  }
  @Test
  public void testChildEntities() throws Exception {
    log.debug("testChildEntities full-import...");
    useParentDeltaQueryParam = random().nextBoolean();
    log.debug("using parent delta? {}", useParentDeltaQueryParam);
    withChildEntities(false, true);
    logPropertiesFile();
    changeStuff();
    log.debug("testChildEntities delta-import...");
    withChildEntities(false, false);
    validateChanges();
  }
    
  
  private int calculateDatabaseCalls() {
    //The main query generates 1
    //Deletes generate 1
    //Each add/mod generate 1
    int c = 1;
    if (countryChanges != null) {
      c += countryChanges.length + 1;
    }
    if (personChanges != null) {
      c += personChanges.addedKeys.length + personChanges.changedKeys.length + 1;
    }
    return c;    
  }
  private void validateChanges() throws Exception
  {
    if(personChanges!=null) {
      for(int id : personChanges.addedKeys) {
        assertQ(req("id:" + id), "//*[@numFound='1']");
      }
      for(int id : personChanges.deletedKeys) {
        assertQ(req("id:" + id), "//*[@numFound='0']");
      }
      for(int id : personChanges.changedKeys) {
        assertQ(req("id:" + id), "//*[@numFound='1']", "substring(//doc/arr[@name='NAME_mult_s']/str[1], 1, 8)='MODIFIED'");
      }
    }
    if(countryChanges!=null) {      
      for(String code : countryChanges) {
        assertQ(req("COUNTRY_CODE_s:" + code), "//*[@numFound='" + numberPeopleByCountryCode(code) + "']", "substring((//doc/str[@name='COUNTRY_NAME_s'])[1], 1, 8)='MODIFIED'");
      }
    }
  }
  private void changeStuff() throws Exception {
    if(countryEntity)
    {
      int n = random().nextInt(2);
      switch(n) {
        case 0:
          personChanges = modifySomePeople();
          break;
        case 1:
          countryChanges = modifySomeCountries();  
          break;
        case 2:
          personChanges = modifySomePeople();
          countryChanges = modifySomeCountries();
          break;
      }
    } else {
      personChanges = modifySomePeople();
    }
    countryChangesLog();
    personChangesLog();
    delta = true;
  }
  private void countryChangesLog() 
  {
    if(countryChanges!=null) {
      StringBuilder sb = new StringBuilder();
      sb.append("country changes { ");
      for(String s : countryChanges) {
        sb.append(s).append(" ");
      }
      sb.append(" }");    
      log.debug("{}", sb);
    }
  }
  private void personChangesLog()
  {
    if(personChanges!=null) {
    log.debug("person changes [ {} ] ", personChanges);
    }
  }
  @Override
  protected LocalSolrQueryRequest generateRequest() {
    return lrf.makeRequest("command", (delta ? "delta-import" : "full-import"), "dataConfig", generateConfig(), 
        "clean", (delta ? "false" : "true"), "commit", "true", "synchronous", "true", "indent", "true");
  }
  @Override
  protected String deltaQueriesPersonTable() {
    return 
        "deletedPkQuery=''SELECT ID FROM PEOPLE WHERE DELETED='Y' AND last_modified &gt;='${dih.People.last_index_time}' '' " +
        "deltaImportQuery=''SELECT ID, NAME, COUNTRY_CODE FROM PEOPLE where ID=${dih.delta.ID} '' " +
        "deltaQuery=''" +
        "SELECT ID FROM PEOPLE WHERE DELETED!='Y' AND last_modified &gt;='${dih.People.last_index_time}' " +
        (useParentDeltaQueryParam ? "" : 
        "UNION DISTINCT " +
        "SELECT ID FROM PEOPLE WHERE DELETED!='Y' AND COUNTRY_CODE IN (SELECT CODE FROM COUNTRIES WHERE last_modified &gt;='${dih.People.last_index_time}') "
        ) + "'' "
    ;
  }
  @Override
  protected String deltaQueriesCountryTable() {
    if(useParentDeltaQueryParam) {
      return 
          "deltaQuery=''SELECT CODE FROM COUNTRIES WHERE DELETED != 'Y' AND last_modified &gt;='${dih.last_index_time}' ''  " +
          "parentDeltaQuery=''SELECT ID FROM PEOPLE WHERE DELETED != 'Y' AND COUNTRY_CODE='${Countries.CODE}' '' "
      ;
          
    }
    return "";
  }
}

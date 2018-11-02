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

import org.junit.Ignore;
import org.junit.Test;

/**
 * Test with various combinations of parameters, child entities, caches, transformers.
 */
public class TestSqlEntityProcessor extends AbstractSqlEntityProcessorTestCase { 
   
  @Test
  public void testSingleEntity() throws Exception {
    singleEntity(1);
  }  
  @Test
  public void testWithSimpleTransformer() throws Exception {
    simpleTransform(1);   
  }
  @Test
  public void testWithComplexTransformer() throws Exception {
    complexTransform(1, 0);
  }
  @Test
  public void testChildEntities() throws Exception {
    withChildEntities(false, true);
  }
  @Test
  public void testCachedChildEntities() throws Exception {
    withChildEntities(true, true);
  }
  
  @Test
  public void testSportZipperChildEntities() throws Exception {
    sportsZipper = true;
    withChildEntities(true, true);
  }

  @Test
  public void testCountryZipperChildEntities() throws Exception {
    countryZipper = true;
    withChildEntities(true, true);
  }
  
  @Test
  public void testBothZipperChildEntities() throws Exception {
    countryZipper = true;
    sportsZipper = true;
    withChildEntities(true, true);
  }
  
  @Test(expected=RuntimeException.class /* DIH exceptions are not propagated, here we capturing assertQ exceptions */)
  public void testSportZipperChildEntitiesWrongOrder() throws Exception {
    if(random().nextBoolean()){
      wrongPeopleOrder = true;
    }else{
      wrongSportsOrder = true;
    }
    testSportZipperChildEntities();
  }

  @Test(expected=RuntimeException.class )
  public void testCountryZipperChildEntitiesWrongOrder() throws Exception {
    if(random().nextBoolean()){
      wrongPeopleOrder = true;
    }else{
      wrongCountryOrder = true;
    }
    testCountryZipperChildEntities();
  }
  
  @Test(expected=RuntimeException.class)
  public void testBothZipperChildEntitiesWrongOrder() throws Exception {
    if(random().nextBoolean()){
      wrongPeopleOrder = true;
    }else{
      if(random().nextBoolean()){
        wrongSportsOrder = true;
      }else{
        wrongCountryOrder = true;
      }
    }
    testBothZipperChildEntities();
  }
  
  @Test
  @Ignore("broken see SOLR-3857")
  public void testSimpleCacheChildEntities() throws Exception {
    simpleCacheChildEntities(true);
  }
   
  @Override
  protected String deltaQueriesCountryTable() {
    return "";
  }
  @Override
  protected String deltaQueriesPersonTable() {
    return "";
  }  
}

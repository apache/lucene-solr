/**
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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class TestThreaded extends AbstractDataImportHandlerTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("dataimport-solrconfig.xml", "dataimport-schema.xml");
  }
  
  @Before
  public void setupData(){
      List parentRow = new ArrayList();
  //  parentRow.add(createMap("id", "1"));
    parentRow.add(createMap("id", "2"));
    parentRow.add(createMap("id", "3"));
    parentRow.add(createMap("id", "4"));
    parentRow.add(createMap("id", "1"));
    MockDataSource.setCollection("select * from x", parentRow);
    // for every x we have four linked y-s
    List childRow = Arrays.asList(
            createMap("desc", "hello"),
            createMap("desc", "bye"),
            createMap("desc", "hi"),
            createMap("desc", "aurevoir"));
    // for N+1 scenario
    MockDataSource.setCollection("select * from y where y.A=1", shuffled(childRow));
    MockDataSource.setCollection("select * from y where y.A=2", shuffled(childRow));
    MockDataSource.setCollection("select * from y where y.A=3", shuffled(childRow));
    MockDataSource.setCollection("select * from y where y.A=4", shuffled(childRow));
    // for cached scenario
    
    List cartesianProduct = new ArrayList();
    for(Map parent : (List<Map>)parentRow){
      for(Iterator<Map> child = shuffled(childRow).iterator(); child.hasNext();){
         Map tuple = createMap("xid", parent.get("id"));
         tuple.putAll(child.next());
         cartesianProduct.add(tuple);
      }
    }
    MockDataSource.setCollection("select * from y", cartesianProduct);
  }
  
  @After
  public void verify() {
    assertQ(req("id:"+ new String[]{"1","2","3","4"}[random.nextInt(4)]),
                      "//*[@numFound='1']");
    assertQ(req("*:*"), "//*[@numFound='4']");
    assertQ(req("desc:hello"), "//*[@numFound='4']");
    assertQ(req("desc:bye"), "//*[@numFound='4']");
    assertQ(req("desc:hi"), "//*[@numFound='4']");
    assertQ(req("desc:aurevoir"), "//*[@numFound='4']");
  }
  
  private <T> List<T> shuffled(List<T> rows){
    ArrayList<T> shuffle = new ArrayList<T>(rows);
    Collections.shuffle(shuffle, random);
    return shuffle;
  }
  
  @Test
  public void testCachedThreadless_FullImport() throws Exception {
    runFullImport(getCachedConfig(random.nextBoolean(), random.nextBoolean(), 0));
  }
  
  @Test
  public void testCachedSingleThread_FullImport() throws Exception {
    runFullImport(getCachedConfig(random.nextBoolean(), random.nextBoolean(), 1));
  }
  
  @Test
  public void testCachedThread_FullImport() throws Exception {
    int numThreads = random.nextInt(8) + 2; // between 2 and 10
    String config = getCachedConfig(random.nextBoolean(), random.nextBoolean(), numThreads);
    runFullImport(config);
  }
    
  private String getCachedConfig(boolean parentCached, boolean childCached, int numThreads) {
    return "<dataConfig>\n"
      +"<dataSource  type=\"MockDataSource\"/>\n"
      + "       <document>\n"
      + "               <entity name=\"x\" "+(numThreads==0 ? "" : "threads=\"" + numThreads + "\"")
      + "                 query=\"select * from x\" " 
      + "                 processor=\""+(parentCached ? "Cached":"")+"SqlEntityProcessor\">\n"
      + "                       <field column=\"id\" />\n"
      + "                       <entity name=\"y\" query=\"select * from y\" where=\"xid=x.id\" " 
      + "                         processor=\""+(childCached ? "Cached":"")+"SqlEntityProcessor\">\n"
      + "                               <field column=\"desc\" />\n"
      + "                       </entity>\n" + "               </entity>\n"
      + "       </document>\n" + "</dataConfig>";
  }
  
}

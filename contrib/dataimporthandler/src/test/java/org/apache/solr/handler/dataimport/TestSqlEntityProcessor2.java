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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Test for SqlEntityProcessor which checks full and delta imports using the
 * test harness
 * </p>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestSqlEntityProcessor2 extends AbstractDataImportHandlerTest {
  @Override
  public String getSchemaFile() {
    return "dataimport-schema.xml";
  }

  @Override
  public String getSolrConfigFile() {
    return "dataimport-solrconfig.xml";
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_FullImport() throws Exception {
    List parentRow = new ArrayList();
    parentRow.add(createMap("id", "1"));
    MockDataSource.setIterator("select * from x", parentRow.iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("desc", "hello"));

    MockDataSource.setIterator("select * from y where y.A=1", childRow
            .iterator());

    super.runFullImport(dataConfig);

    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("desc:hello"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_DeltaImport() throws Exception {
    List deltaRow = new ArrayList();
    deltaRow.add(createMap("id", "5"));
    MockDataSource.setIterator("select id from x where last_modified > NOW",
            deltaRow.iterator());

    List parentRow = new ArrayList();
    parentRow.add(createMap("id", "5"));
    MockDataSource.setIterator("select * from x where x.id = '5'", parentRow
            .iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("desc", "hello"));
    MockDataSource.setIterator("select * from y where y.A=5", childRow
            .iterator());

    super.runDeltaImport(dataConfig);

    assertQ(req("id:5"), "//*[@numFound='1']");
    assertQ(req("desc:hello"), "//*[@numFound='1']");
  }

  private static String dataConfig = "<dataConfig>\n"
          + "       <document>\n"
          + "               <entity name=\"x\" pk=\"x.id\" query=\"select * from x\" deltaQuery=\"select id from x where last_modified > NOW\">\n"
          + "                       <field column=\"id\" />\n"
          + "                       <entity name=\"y\" query=\"select * from y where y.A=${x.id}\">\n"
          + "                               <field column=\"desc\" />\n"
          + "                       </entity>\n" + "               </entity>\n"
          + "       </document>\n" + "</dataConfig>\n";
}

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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat;
import java.text.ParseException;

/**
 * <p>
 * Test for SqlEntityProcessor which checks full and delta imports using the
 * test harness
 * </p>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestSqlEntityProcessor2 extends AbstractDataImportHandlerTestCase {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("dataimport-solrconfig.xml", "dataimport-schema.xml");
  }

  @Before @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
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

    runFullImport(dataConfig);

    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("desc:hello"), "//*[@numFound='1']");
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_FullImport_MT() throws Exception {
    List parentRow = new ArrayList();
    parentRow.add(createMap("id", "1"));
    parentRow.add(createMap("id", "2"));
    MockDataSource.setIterator("select * from x", parentRow.iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("desc", "hello"));

    MockDataSource.setIterator("select * from y where y.A=1", childRow.iterator());
    MockDataSource.setIterator("select * from y where y.A=2", childRow.iterator());

    runFullImport(dataConfig_2threads);

    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("id:2"), "//*[@numFound='1']");
    assertQ(req("desc:hello"), "//*[@numFound='2']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_FullImportNoCommit() throws Exception {
    List parentRow = new ArrayList();
    parentRow.add(createMap("id", "10"));
    MockDataSource.setIterator("select * from x", parentRow.iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("desc", "hello"));

    MockDataSource.setIterator("select * from y where y.A=10", childRow
            .iterator());


    runFullImport(dataConfig,createMap("commit","false"));
    assertQ(req("id:10"), "//*[@numFound='0']");
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
    MockDataSource.setIterator("select * from x where id = '5'", parentRow
            .iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("desc", "hello"));
    MockDataSource.setIterator("select * from y where y.A=5", childRow
            .iterator());

    runDeltaImport(dataConfig);

    assertQ(req("id:5"), "//*[@numFound='1']");
    assertQ(req("desc:hello"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_DeltaImport_DeletedPkQuery() throws Exception {
    List parentRow = new ArrayList();
    parentRow.add(createMap("id", "11"));
    MockDataSource.setIterator("select * from x", parentRow.iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("desc", "hello"));

    MockDataSource.setIterator("select * from y where y.A=11", childRow
            .iterator());

    runFullImport(dataConfig);

    assertQ(req("id:11"), "//*[@numFound='1']");



    List deltaRow = new ArrayList();
    deltaRow.add(createMap("id", "15"));
    deltaRow.add(createMap("id", "17"));
    MockDataSource.setIterator("select id from x where last_modified > NOW",
            deltaRow.iterator());

    List deltaDeleteRow = new ArrayList();
    deltaDeleteRow.add(createMap("id", "11"));
    deltaDeleteRow.add(createMap("id", "17"));
    MockDataSource.setIterator("select id from x where last_modified > NOW AND deleted='true'",
            deltaDeleteRow.iterator());

    parentRow = new ArrayList();
    parentRow.add(createMap("id", "15"));
    MockDataSource.setIterator("select * from x where id = '15'", parentRow
            .iterator());

    parentRow = new ArrayList();
    parentRow.add(createMap("id", "17"));
    MockDataSource.setIterator("select * from x where id = '17'", parentRow
            .iterator());

    runDeltaImport(dataConfig);

    assertQ(req("id:15"), "//*[@numFound='1']");
    assertQ(req("id:11"), "//*[@numFound='0']");
    assertQ(req("id:17"), "//*[@numFound='0']");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompositePk_DeltaImport_DeltaImportQuery() throws Exception {
    List deltaRow = new ArrayList();
    deltaRow.add(createMap("id", "5"));
    MockDataSource.setIterator("select id from x where last_modified > NOW",
            deltaRow.iterator());

    List parentRow = new ArrayList();
    parentRow.add(createMap("id", "5"));
    MockDataSource.setIterator("select * from x where id=5", parentRow
            .iterator());

    List childRow = new ArrayList();
    childRow.add(createMap("desc", "hello"));
    MockDataSource.setIterator("select * from y where y.A=5", childRow
            .iterator());

    runDeltaImport(dataConfig_deltaimportquery);

    assertQ(req("id:5"), "//*[@numFound='1']");
    assertQ(req("desc:hello"), "//*[@numFound='1']");
  }

  @Test
  @SuppressWarnings("unchecked")
  @Ignore("Known Locale/TZ problems: see https://issues.apache.org/jira/browse/SOLR-1916")
  public void testLastIndexTime() throws Exception  {
    List row = new ArrayList();
    row.add(createMap("id", 5));
    MockDataSource.setIterator("select * from x where last_modified > OK", row.iterator());
    runFullImport(dataConfig_LastIndexTime);
    assertQ(req("id:5"), "//*[@numFound='1']");
  }

  static class DateFormatValidatingEvaluator extends Evaluator {
    @Override
    public String evaluate(String expression, Context context) {
      List l = EvaluatorBag.parseParams(expression, context.getVariableResolver());
      Object o = l.get(0);
      String dateStr = null;
      if (o instanceof EvaluatorBag.VariableWrapper) {
        EvaluatorBag.VariableWrapper wrapper = (EvaluatorBag.VariableWrapper) o;
        o = wrapper.resolve();
        dateStr = o.toString();
      }
      SimpleDateFormat formatter = DataImporter.DATE_TIME_FORMAT.get();
      try {
        formatter.parse(dateStr);
      } catch (ParseException e) {
        DataImportHandlerException.wrapAndThrow(DataImportHandlerException.SEVERE, e);
      }
      return "OK";
    }
  }

  private static String dataConfig_LastIndexTime = "<dataConfig><dataSource  type=\"MockDataSource\"/>\n" +
          "\t<function name=\"checkDateFormat\" class=\"org.apache.solr.handler.dataimport.TestSqlEntityProcessor2$DateFormatValidatingEvaluator\"/>\n" +
          "\t<document>\n" +
          "\t\t<entity name=\"x\" query=\"select * from x where last_modified > ${dih.functions.checkDateFormat(dih.last_index_time)}\" />\n" +
          "\t</document>\n" +
          "</dataConfig>";

  private static String dataConfig = "<dataConfig><dataSource  type=\"MockDataSource\"/>\n"
          + "       <document>\n"
          + "               <entity name=\"x\" pk=\"id\" query=\"select * from x\" deletedPkQuery=\"select id from x where last_modified > NOW AND deleted='true'\" deltaQuery=\"select id from x where last_modified > NOW\">\n"
          + "                       <field column=\"id\" />\n"
          + "                       <entity name=\"y\" query=\"select * from y where y.A=${x.id}\">\n"
          + "                               <field column=\"desc\" />\n"
          + "                       </entity>\n" + "               </entity>\n"
          + "       </document>\n" + "</dataConfig>\n";

  private static String dataConfig_2threads = "<dataConfig><dataSource  type=\"MockDataSource\"/>\n"
          + "       <document>\n"
          + "               <entity name=\"x\" pk=\"id\" query=\"select * from x\" threads=\"2\">\n"
          + "                       <field column=\"id\" />\n"
          + "                       <entity name=\"y\" query=\"select * from y where y.A=${x.id}\">\n"
          + "                               <field column=\"desc\" />\n"
          + "                       </entity>\n" + "               </entity>\n"
          + "       </document>\n" + "</dataConfig>\n";

  private static String dataConfig_deltaimportquery = "<dataConfig><dataSource  type=\"MockDataSource\"/>\n"
          + "       <document>\n"
          + "               <entity name=\"x\" deltaImportQuery=\"select * from x where id=${dataimporter.delta.id}\" deltaQuery=\"select id from x where last_modified > NOW\">\n"
          + "                       <field column=\"id\" />\n"
          + "                       <entity name=\"y\" query=\"select * from y where y.A=${x.id}\">\n"
          + "                               <field column=\"desc\" />\n"
          + "                       </entity>\n" + "               </entity>\n"
          + "       </document>\n" + "</dataConfig>\n";
}

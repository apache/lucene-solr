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
 *
 * @since solr 1.3
 */
@Ignore("FIXME: I fail so often it makes me ill!")
public class TestSqlEntityProcessor2 extends AbstractDataImportHandlerTestCase {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("dataimport-solrconfig.xml", "dataimport-schema.xml");
  }
  
  @Before 
  @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }
  
  @Test
  @SuppressWarnings("unchecked")
  @Ignore("Known Locale/TZ problems: see https://issues.apache.org/jira/browse/SOLR-1916")
  /**
   * This test is here for historical purposes only.  
   * When SOLR-1916 is fixed, it would be best to rewrite this test.
   * 
   * @throws Exception
   */
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
}

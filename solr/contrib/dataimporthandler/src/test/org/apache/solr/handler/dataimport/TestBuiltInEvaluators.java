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
import org.junit.Ignore;
import org.junit.Test;

import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * <p> Test for Evaluators </p>
 *
 *
 * @since solr 1.3
 */
public class TestBuiltInEvaluators extends AbstractDataImportHandlerTestCase {
  private static final String ENCODING = "UTF-8";

  VariableResolver resolver;

  Map<String, String> sqlTests;

  Map<String, String> urlTests;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    resolver = new VariableResolver();

    sqlTests = new HashMap<String, String>();

    sqlTests.put("foo\"", "foo\"\"");
    sqlTests.put("foo\\", "foo\\\\");
    sqlTests.put("foo'", "foo''");
    sqlTests.put("foo''", "foo''''");
    sqlTests.put("'foo\"", "''foo\"\"");
    sqlTests.put("\"Albert D'souza\"", "\"\"Albert D''souza\"\"");

    urlTests = new HashMap<String, String>();

    urlTests.put("*:*", URLEncoder.encode("*:*", ENCODING));
    urlTests.put("price:[* TO 200]", URLEncoder.encode("price:[* TO 200]",
            ENCODING));
    urlTests.put("review:\"hybrid sedan\"", URLEncoder.encode(
            "review:\"hybrid sedan\"", ENCODING));
  }

  
  @Test
  public void testSqlEscapingEvaluator() {
    Evaluator sqlEscaper = new SqlEscapingEvaluator();
    runTests(sqlTests, sqlEscaper);
  }

  
  @Test
  public void testUrlEvaluator() throws Exception {
    Evaluator urlEvaluator = new UrlEvaluator();
    runTests(urlTests, urlEvaluator);
  }

  @Test
  public void parseParams() {
    Map<String,Object> m = new HashMap<String,Object>();
    m.put("b","B");
    VariableResolver vr = new VariableResolver();
    vr.addNamespace("a",m);
    List<Object> l = (new Evaluator() {      
      @Override
      public String evaluate(String expression, Context context) {
        return null;
      }
    }).parseParams(" 1 , a.b, 'hello!', 'ds,o,u\'za',",vr);
    assertEquals(new Double(1),l.get(0));
    assertEquals("B",((Evaluator.VariableWrapper)l.get(1)).resolve());
    assertEquals("hello!",l.get(2));
    assertEquals("ds,o,u'za",l.get(3));
  }

  @Test
  public void testEscapeSolrQueryFunction() {
    final VariableResolver resolver = new VariableResolver();    
    Map<String,Object> m= new HashMap<String,Object>();
    m.put("query","c:t");
    resolver.setEvaluators(new DataImporter().getEvaluators(Collections.<Map<String,String>>emptyList()));
    
    resolver.addNamespace("e",m);
    String s = resolver
            .replaceTokens("${dataimporter.functions.escapeQueryChars(e.query)}");
    org.junit.Assert.assertEquals("c\\:t", s);
    
  }
  
  private Date getNow() {
    Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"),
        Locale.ROOT);
    calendar.add(Calendar.DAY_OF_YEAR, -2);
    return calendar.getTime();
  }
  
  @Test
  @Ignore("fails if somewhere on earth is a DST change")
  public void testDateFormatEvaluator() {
    Evaluator dateFormatEval = new DateFormatEvaluator();
    ContextImpl context = new ContextImpl(null, resolver, null,
        Context.FULL_DUMP, Collections.<String,Object> emptyMap(), null, null);
    String currentLocale = Locale.getDefault().toString();
    {
      {
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH", Locale.ROOT);
        String sdf = sdfDate.format(getNow());
        String dfe = dateFormatEval.evaluate("'NOW-2DAYS','yyyy-MM-dd HH'", context);
        assertEquals(sdf,dfe);
      }
      {
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH", Locale.getDefault());
        String sdf = sdfDate.format(getNow());
        String dfe = dateFormatEval.evaluate("'NOW-2DAYS','yyyy-MM-dd HH','"+ currentLocale + "'", context);
        assertEquals(sdf,dfe);
        for(String tz : TimeZone.getAvailableIDs()) {          
          sdfDate.setTimeZone(TimeZone.getTimeZone(tz));
          sdf = sdfDate.format(getNow());
          dfe = dateFormatEval.evaluate("'NOW-2DAYS','yyyy-MM-dd HH','" + currentLocale + "','" + tz + "'", context);
          assertEquals(sdf,dfe);          
        }
      }
    }
    Date d = new Date();    
    Map<String,Object> map = new HashMap<String,Object>();
    map.put("key", d);
    resolver.addNamespace("A", map);
    
    assertEquals(
        new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.ROOT).format(d),
        dateFormatEval.evaluate("A.key, 'yyyy-MM-dd HH:mm'", context));
    assertEquals(
        new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.getDefault()).format(d),
        dateFormatEval.evaluate("A.key, 'yyyy-MM-dd HH:mm','" + currentLocale
            + "'", context));
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.getDefault());
    for(String tz : TimeZone.getAvailableIDs()) {
      sdf.setTimeZone(TimeZone.getTimeZone(tz));
      assertEquals(
          sdf.format(d),
          dateFormatEval.evaluate("A.key, 'yyyy-MM-dd HH:mm','" + currentLocale + "', '" + tz + "'", context));     
      
    }
    
    
  }

  private void runTests(Map<String, String> tests, Evaluator evaluator) {
    ContextImpl ctx = new ContextImpl(null, resolver, null, Context.FULL_DUMP, Collections.<String, Object>emptyMap(), null, null);    
    for (Map.Entry<String, String> entry : tests.entrySet()) {
      Map<String, Object> values = new HashMap<String, Object>();
      values.put("key", entry.getKey());
      resolver.addNamespace("A", values);

      String expected = entry.getValue();
      String actual = evaluator.evaluate("A.key", ctx);
      assertEquals(expected, actual);
    }
    
  }
}

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

import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.*;

import junit.framework.Assert;

/**
 * <p> Test for EvaluatorBag </p>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestEvaluatorBag {
  private static final String ENCODING = "UTF-8";

  VariableResolverImpl resolver;

  Map<String, String> sqlTests;

  Map<String, String> urlTests;

  @Before
  public void setUp() throws Exception {
    resolver = new VariableResolverImpl();

    sqlTests = new HashMap<String, String>();

    sqlTests.put("foo\"", "foo\"\"");
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

  /**
   * Test method for {@link EvaluatorBag#getSqlEscapingEvaluator()}.
   */
  @Test
  public void testGetSqlEscapingEvaluator() {
    Evaluator sqlEscaper = EvaluatorBag.getSqlEscapingEvaluator();
    runTests(sqlTests, sqlEscaper);
  }

  /**
   * Test method for {@link EvaluatorBag#getUrlEvaluator()}.
   */
  @Test
  public void testGetUrlEvaluator() throws Exception {
    Evaluator urlEvaluator = EvaluatorBag.getUrlEvaluator();
    runTests(urlTests, urlEvaluator);
  }

  @Test
  public void parseParams() {
    Map m = new HashMap();
    m.put("b","B");
    VariableResolverImpl vr = new VariableResolverImpl();
    vr.addNamespace("a",m);
    List l =  EvaluatorBag.parseParams(" 1 , a.b, 'hello!', 'ds,o,u\'za',",vr);
    Assert.assertEquals(new Double(1),l.get(0));
    Assert.assertEquals("B",((EvaluatorBag.VariableWrapper)l.get(1)).resolve());
    Assert.assertEquals("hello!",l.get(2));
    Assert.assertEquals("ds,o,u'za",l.get(3));
  }

  @Test
  public void testEscapeSolrQueryFunction() {
    final VariableResolverImpl resolver = new VariableResolverImpl();
    ContextImpl context = new ContextImpl(null, resolver, null, Context.FULL_DUMP, Collections.EMPTY_MAP, null, null);
    Context.CURRENT_CONTEXT.set(context);
    try {
      Map m= new HashMap();
      m.put("query","c:t");
      resolver.addNamespace("dataimporter.functions", EvaluatorBag
              .getFunctionsNamespace(Collections.EMPTY_LIST, null));
      resolver.addNamespace("e",m);
      String s = resolver
              .replaceTokens("${dataimporter.functions.escapeQueryChars(e.query)}");
      org.junit.Assert.assertEquals("c\\:t", s);
    } finally {
      Context.CURRENT_CONTEXT.remove();
    }
  }

  /**
   * Test method for {@link EvaluatorBag#getDateFormatEvaluator()}.
   */
  @Test
  public void testGetDateFormatEvaluator() {
    Evaluator dateFormatEval = EvaluatorBag.getDateFormatEvaluator();
    ContextImpl context = new ContextImpl(null, resolver, null, Context.FULL_DUMP, Collections.EMPTY_MAP, null, null);
    Context.CURRENT_CONTEXT.set(context);
    try {
      long time = System.currentTimeMillis();
      assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(time - 2*86400*1000)),
              dateFormatEval.evaluate("'NOW-2DAYS','yyyy-MM-dd HH:mm'", Context.CURRENT_CONTEXT.get()));

      Map<String, Object> map = new HashMap<String, Object>();
      map.put("key", new Date(time));
      resolver.addNamespace("A", map);

      assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(time)),
              dateFormatEval.evaluate("A.key, 'yyyy-MM-dd HH:mm'", Context.CURRENT_CONTEXT.get()));
    } finally {
      Context.CURRENT_CONTEXT.remove();
    }
  }

  private void runTests(Map<String, String> tests, Evaluator evaluator) {
    ContextImpl ctx = new ContextImpl(null, resolver, null, Context.FULL_DUMP, Collections.EMPTY_MAP, null, null);
    Context.CURRENT_CONTEXT.set(ctx);
    try {
      for (Map.Entry<String, String> entry : tests.entrySet()) {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("key", entry.getKey());
        resolver.addNamespace("A", values);

        String expected = (String) entry.getValue();
        String actual = evaluator.evaluate("A.key", ctx);
        assertEquals(expected, actual);
      }
    } finally {
      Context.CURRENT_CONTEXT.remove();
    }
  }
}

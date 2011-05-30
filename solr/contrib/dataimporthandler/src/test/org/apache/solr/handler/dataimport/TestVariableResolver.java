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
import org.apache.solr.util.DateMathParser;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * <p>
 * Test for VariableResolver
 * </p>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class TestVariableResolver extends AbstractDataImportHandlerTestCase {

  @Test
  public void testSimpleNamespace() {
    VariableResolverImpl vri = new VariableResolverImpl();
    Map<String, Object> ns = new HashMap<String, Object>();
    ns.put("world", "WORLD");
    vri.addNamespace("hello", ns);
    assertEquals("WORLD", vri.resolve("hello.world"));
  }

  @Test
  public void testDefaults(){
    // System.out.println(System.setProperty(TestVariableResolver.class.getName(),"hello"));
    System.setProperty(TestVariableResolver.class.getName(),"hello");
    // System.out.println("s.gP()"+ System.getProperty(TestVariableResolver.class.getName())); 

    HashMap m = new HashMap();
    m.put("hello","world");
    VariableResolverImpl vri = new VariableResolverImpl(m);
    Object val = vri.resolve(TestVariableResolver.class.getName());
    // System.out.println("val = " + val);
    assertEquals("hello", val);
    assertEquals("world",vri.resolve("hello"));
  }

  @Test
  public void testNestedNamespace() {
    VariableResolverImpl vri = new VariableResolverImpl();
    Map<String, Object> ns = new HashMap<String, Object>();
    ns.put("world", "WORLD");
    vri.addNamespace("hello", ns);
    ns = new HashMap<String, Object>();
    ns.put("world1", "WORLD1");
    vri.addNamespace("hello.my", ns);
    assertEquals("WORLD1", vri.resolve("hello.my.world1"));
  }

  @Test
  public void test3LevelNestedNamespace() {
    VariableResolverImpl vri = new VariableResolverImpl();
    Map<String, Object> ns = new HashMap<String, Object>();
    ns.put("world", "WORLD");
    vri.addNamespace("hello", ns);
    ns = new HashMap<String, Object>();
    ns.put("world1", "WORLD1");
    vri.addNamespace("hello.my.new", ns);
    assertEquals("WORLD1", vri.resolve("hello.my.new.world1"));
  }

  @Test
  public void dateNamespaceWithValue() {
    VariableResolverImpl vri = new VariableResolverImpl();
    ContextImpl context = new ContextImpl(null, vri, null, Context.FULL_DUMP, Collections.EMPTY_MAP, null, null);
    Context.CURRENT_CONTEXT.set(context);
    try {
      vri.addNamespace("dataimporter.functions", EvaluatorBag
              .getFunctionsNamespace(Collections.EMPTY_LIST, null));
      Map<String, Object> ns = new HashMap<String, Object>();
      Date d = new Date();
      ns.put("dt", d);
      vri.addNamespace("A", ns);
      assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(d),
                      vri.replaceTokens("${dataimporter.functions.formatDate(A.dt,'yyyy-MM-dd HH:mm:ss')}"));
    } finally {
      Context.CURRENT_CONTEXT.remove();
    }
  }

  @Test
  public void dateNamespaceWithExpr() throws Exception {
    VariableResolverImpl vri = new VariableResolverImpl();
    ContextImpl context = new ContextImpl(null, vri, null, Context.FULL_DUMP, Collections.EMPTY_MAP, null, null);
    Context.CURRENT_CONTEXT.set(context);
    try {
      vri.addNamespace("dataimporter.functions", EvaluatorBag
              .getFunctionsNamespace(Collections.EMPTY_LIST,null));

      SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
      format.setTimeZone(TimeZone.getTimeZone("UTC"));
      resetEvaluatorBagDateMathParser();
      DateMathParser dmp = new DateMathParser(TimeZone.getDefault(), Locale.getDefault());

      String s = vri.replaceTokens("${dataimporter.functions.formatDate('NOW/DAY','yyyy-MM-dd HH:mm')}");
      assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm").format(dmp.parseMath("/DAY")), s);
    } finally {
      Context.CURRENT_CONTEXT.remove();
    }
  }

  @Test
  public void testDefaultNamespace() {
    VariableResolverImpl vri = new VariableResolverImpl();
    Map<String, Object> ns = new HashMap<String, Object>();
    ns.put("world", "WORLD");
    vri.addNamespace(null, ns);
    assertEquals("WORLD", vri.resolve("world"));
  }

  @Test
  public void testDefaultNamespace1() {
    VariableResolverImpl vri = new VariableResolverImpl();
    Map<String, Object> ns = new HashMap<String, Object>();
    ns.put("world", "WORLD");
    vri.addNamespace(null, ns);
    assertEquals("WORLD", vri.resolve("world"));
  }

  @Test
  public void testFunctionNamespace1() throws Exception {
    VariableResolverImpl resolver = new VariableResolverImpl();
    ContextImpl context = new ContextImpl(null, resolver, null, Context.FULL_DUMP, Collections.EMPTY_MAP, null, null);
    Context.CURRENT_CONTEXT.set(context);
    try {
      final List<Map<String ,String >> l = new ArrayList<Map<String, String>>();
      Map<String ,String > m = new HashMap<String, String>();
      m.put("name","test");
      m.put("class",E.class.getName());
      l.add(m);

      SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
      format.setTimeZone(TimeZone.getTimeZone("UTC"));
      resetEvaluatorBagDateMathParser();
      DateMathParser dmp = new DateMathParser(TimeZone.getDefault(), Locale.getDefault());

      resolver.addNamespace("dataimporter.functions", EvaluatorBag
              .getFunctionsNamespace(l,null));
      String s = resolver
              .replaceTokens("${dataimporter.functions.formatDate('NOW/DAY','yyyy-MM-dd HH:mm')}");
      assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm")
              .format(dmp.parseMath("/DAY")), s);
      assertEquals("Hello World", resolver
              .replaceTokens("${dataimporter.functions.test('TEST')}"));
    } finally {
      Context.CURRENT_CONTEXT.remove();
    }
  }

  public static class E extends Evaluator{
      @Override
      public String evaluate(String expression, Context context) {
        return "Hello World";
      }
  }

  private void resetEvaluatorBagDateMathParser() {
    EvaluatorBag.dateMathParser = new DateMathParser(TimeZone
            .getDefault(), Locale.getDefault()){
      @Override
      public Date getNow() {
        return new Date();
      }
    };
  }
}

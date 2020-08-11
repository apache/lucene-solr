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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.solr.util.DateMathParser;
import org.junit.Test;

/**
 * <p>
 * Test for VariableResolver
 * </p>
 * 
 * 
 * @since solr 1.3
 */
public class TestVariableResolver extends AbstractDataImportHandlerTestCase {
  
  @Test
  public void testSimpleNamespace() {
    VariableResolver vri = new VariableResolver();
    Map<String,Object> ns = new HashMap<>();
    ns.put("world", "WORLD");
    vri.addNamespace("hello", ns);
    assertEquals("WORLD", vri.resolve("hello.world"));
  }
  
  @Test
  public void testDefaults() {
    // System.out.println(System.setProperty(TestVariableResolver.class.getName(),"hello"));
    System.setProperty(TestVariableResolver.class.getName(), "hello");
    // System.out.println("s.gP()"+
    // System.getProperty(TestVariableResolver.class.getName()));
    
    Properties p = new Properties();
    p.put("hello", "world");
    VariableResolver vri = new VariableResolver(p);
    Object val = vri.resolve(TestVariableResolver.class.getName());
    // System.out.println("val = " + val);
    assertEquals("hello", val);
    assertEquals("world", vri.resolve("hello"));
  }
  
  @Test
  public void testNestedNamespace() {
    VariableResolver vri = new VariableResolver();
    Map<String,Object> ns = new HashMap<>();
    ns.put("world", "WORLD");
    vri.addNamespace("hello", ns);
    ns = new HashMap<>();
    ns.put("world1", "WORLD1");
    vri.addNamespace("hello.my", ns);
    assertEquals("WORLD1", vri.resolve("hello.my.world1"));
  }
  
  @Test
  public void test3LevelNestedNamespace() {
    VariableResolver vri = new VariableResolver();
    Map<String,Object> ns = new HashMap<>();
    ns.put("world", "WORLD");
    vri.addNamespace("hello", ns);
    ns = new HashMap<>();
    ns.put("world1", "WORLD1");
    vri.addNamespace("hello.my.new", ns);
    assertEquals("WORLD1", vri.resolve("hello.my.new.world1"));
  }
  
  @Test
  public void dateNamespaceWithValue() {
    VariableResolver vri = new VariableResolver();
    vri.setEvaluators(new DataImporter().getEvaluators(Collections
        .<Map<String,String>> emptyList()));
    Map<String,Object> ns = new HashMap<>();
    Date d = new Date();
    ns.put("dt", d);
    vri.addNamespace("A", ns);
    assertEquals(
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT).format(d),
        vri.replaceTokens("${dataimporter.functions.formatDate(A.dt,'yyyy-MM-dd HH:mm:ss')}"));
  }
  
  @Test
  public void dateNamespaceWithExpr() throws Exception {
    VariableResolver vri = new VariableResolver();
    vri.setEvaluators(new DataImporter().getEvaluators(Collections
        .<Map<String,String>> emptyList()));
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT);
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    DateMathParser dmp = new DateMathParser(TimeZone.getDefault());
    
    String s = vri
        .replaceTokens("${dataimporter.functions.formatDate('NOW/DAY','yyyy-MM-dd HH:mm')}");
    assertEquals(
        new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.ROOT).format(dmp.parseMath("/DAY")),
        s);
  }
  
  @Test
  public void testDefaultNamespace() {
    VariableResolver vri = new VariableResolver();
    Map<String,Object> ns = new HashMap<>();
    ns.put("world", "WORLD");
    vri.addNamespace(null, ns);
    assertEquals("WORLD", vri.resolve("world"));
  }
  
  @Test
  public void testDefaultNamespace1() {
    VariableResolver vri = new VariableResolver();
    Map<String,Object> ns = new HashMap<>();
    ns.put("world", "WORLD");
    vri.addNamespace(null, ns);
    assertEquals("WORLD", vri.resolve("world"));
  }
  
  @Test
  public void testFunctionNamespace1() throws Exception {
    VariableResolver resolver = new VariableResolver();
    final List<Map<String,String>> l = new ArrayList<>();
    Map<String,String> m = new HashMap<>();
    m.put("name", "test");
    m.put("class", E.class.getName());
    l.add(m);
    resolver.setEvaluators(new DataImporter().getEvaluators(l));
    @SuppressWarnings({"unchecked"})
    ContextImpl context = new ContextImpl(null, resolver, null,
        Context.FULL_DUMP, Collections.EMPTY_MAP, null, null);
    
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT);
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    DateMathParser dmp = new DateMathParser(TimeZone.getDefault());
    
    String s = resolver
        .replaceTokens("${dataimporter.functions.formatDate('NOW/DAY','yyyy-MM-dd HH:mm')}");
    assertEquals(
        new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.ROOT).format(dmp.parseMath("/DAY")),
        s);
    assertEquals("Hello World",
        resolver.replaceTokens("${dataimporter.functions.test('TEST')}"));
  }
  
  public static class E extends Evaluator {
    @Override
    public String evaluate(String expression, Context context) {
      return "Hello World";
    }
  }
  
}

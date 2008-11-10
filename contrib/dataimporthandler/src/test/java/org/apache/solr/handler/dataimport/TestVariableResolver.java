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

import org.junit.Assert;
import org.junit.Test;

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
public class TestVariableResolver {

  @Test
  public void testSimpleNamespace() {
    VariableResolverImpl vri = new VariableResolverImpl();
    Map<String, Object> ns = new HashMap<String, Object>();
    ns.put("world", "WORLD");
    vri.addNamespace("hello", ns);
    Assert.assertEquals("WORLD", vri.resolve("hello.world"));
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
    Assert.assertEquals("WORLD1", vri.resolve("hello.my.world1"));
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
    Assert.assertEquals("WORLD1", vri.resolve("hello.my.new.world1"));
  }

  @Test
  public void dateNamespaceWithValue() {
    VariableResolverImpl vri = new VariableResolverImpl();
    vri.addNamespace("dataimporter.functions", EvaluatorBag
            .getFunctionsNamespace(vri, Collections.EMPTY_LIST, null));
    Map<String, Object> ns = new HashMap<String, Object>();
    Date d = new Date();
    ns.put("dt", d);
    vri.addNamespace("A", ns);
    Assert
            .assertEquals(
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(d),
                    vri
                            .replaceTokens("${dataimporter.functions.formatDate(A.dt,yyyy-MM-dd HH:mm:ss)}"));
  }

  @Test
  public void dateNamespaceWithExpr() {
    VariableResolverImpl vri = new VariableResolverImpl();
    vri.addNamespace("dataimporter.functions", EvaluatorBag
            .getFunctionsNamespace(vri, Collections.EMPTY_LIST,null));
    String s = vri
            .replaceTokens("${dataimporter.functions.formatDate('NOW',yyyy-MM-dd HH:mm)}");
    Assert.assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm")
            .format(new Date()), s);
  }

  @Test
  public void testDefaultNamespace() {
    VariableResolverImpl vri = new VariableResolverImpl();
    Map<String, Object> ns = new HashMap<String, Object>();
    ns.put("world", "WORLD");
    vri.addNamespace(null, ns);
    Assert.assertEquals("WORLD", vri.resolve("world"));
  }

  @Test
  public void testDefaultNamespace1() {
    VariableResolverImpl vri = new VariableResolverImpl();
    Map<String, Object> ns = new HashMap<String, Object>();
    ns.put("world", "WORLD");
    vri.addNamespace(null, ns);
    Assert.assertEquals("WORLD", vri.resolve("world"));
  }

  @Test
  public void testFunctionNamespace1() {
    final VariableResolverImpl resolver = new VariableResolverImpl();
    final List<Map<String ,String >> l = new ArrayList<Map<String, String>>();
    Map<String ,String > m = new HashMap<String, String>();
    m.put("name","test");
    m.put("class",E.class.getName());
    l.add(m);
    resolver.addNamespace("dataimporter.functions", EvaluatorBag
            .getFunctionsNamespace(resolver, l,null));
    String s = resolver
            .replaceTokens("${dataimporter.functions.formatDate('NOW',yyyy-MM-dd HH:mm)}");
    Assert.assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm")
            .format(new Date()), s);
    Assert.assertEquals("Hello World", resolver
            .replaceTokens("${dataimporter.functions.test('TEST')}"));
  }

  public static class E extends Evaluator{
      public String evaluate(VariableResolver resolver, String expression) {
        return "Hello World";
      }
  }
}

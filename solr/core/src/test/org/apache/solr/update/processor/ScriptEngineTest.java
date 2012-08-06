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

package org.apache.solr.update.processor;

import org.apache.lucene.util.LuceneTestCase;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.StringReader;

import org.junit.Assume;
import org.junit.BeforeClass;

/**
 * Sanity tests basic functionality of {@link ScriptEngineManager} and 
 * {@link ScriptEngine} w/o excercising any Lucene specific code.
 */
public class ScriptEngineTest extends LuceneTestCase {

  private ScriptEngineManager manager;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Assume.assumeNotNull((new ScriptEngineManager()).getEngineByExtension("js"));
    Assume.assumeNotNull((new ScriptEngineManager()).getEngineByName("JavaScript"));
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    manager = new ScriptEngineManager();
  }

  public void testGetEngineByName() {
    ScriptEngine engine = manager.getEngineByName("JavaScript");
    assertNotNull(engine);
    engine = manager.getEngineByName("DummyScript");
    assertNull(engine);
  }

  public void testGetEngineByExtension() {
    ScriptEngine engine = manager.getEngineByExtension("js");
    assertNotNull(engine);
    engine = manager.getEngineByExtension("foobar");
    assertNull(engine);
  }

  public void testEvalText() throws ScriptException, NoSuchMethodException {
    ScriptEngine engine = manager.getEngineByName("JavaScript");
    assertNotNull(engine);
    engine.eval("function add(a,b) { return a + b }");
    Double result = (Double) ((Invocable)engine).invokeFunction("add", 1, 2);
    assertNotNull(result);
    assertEquals(3, result.intValue());
  }

  public void testEvalReader() throws ScriptException, NoSuchMethodException {
    ScriptEngine engine = manager.getEngineByName("JavaScript");
    assertNotNull(engine);
    StringReader reader = new StringReader("function add(a,b) { return a + b }");
    engine.eval(reader);
    Double result = (Double) ((Invocable)engine).invokeFunction("add", 1, 2);
    assertNotNull(result);
    assertEquals(3, result.intValue());
  }

  public void testPut() throws ScriptException, NoSuchMethodException {
    manager.put("a", 1);
    ScriptEngine engine = manager.getEngineByName("JavaScript");
    engine.put("b", 2);
    assertNotNull(engine);
    engine.eval("function add() { return a + b }");
    Double result = (Double) ((Invocable)engine).invokeFunction("add", 1, 2);
    assertNotNull(result);
    assertEquals(3, result.intValue());
  }

 public void testJRuby() throws ScriptException, NoSuchMethodException {  
   // Simply adding jruby.jar to Solr's lib/ directory gets this test passing
   ScriptEngine engine = manager.getEngineByName("jruby");

   Assume.assumeNotNull(engine);

   assertNotNull(engine);
   engine.eval("def add(a,b); a + b; end");
   Long result = (Long) ((Invocable)engine).invokeFunction("add", 1, 2);
   assertNotNull(result);
   assertEquals(3, result.intValue());
 }

}

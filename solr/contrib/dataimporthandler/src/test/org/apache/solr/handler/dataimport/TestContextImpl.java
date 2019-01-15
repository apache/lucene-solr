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

import java.util.HashMap;

import org.junit.Test;

public class TestContextImpl extends AbstractDataImportHandlerTestCase {
  
  @Test
  public void testEntityScope() {
    ContextImpl ctx = new ContextImpl(null, new VariableResolver(), null, "something", new HashMap<String,Object>(), null, null);
    String lala = new String("lala");
    ctx.setSessionAttribute("huhu", lala, Context.SCOPE_ENTITY);
    Object got = ctx.getSessionAttribute("huhu", Context.SCOPE_ENTITY);
    
    assertEquals(lala, got);
    
  }
  @Test
  public void testCoreScope() {
    DataImporter di = new DataImporter();
    di.loadAndInit("<dataConfig><document /></dataConfig>");
    DocBuilder db = new DocBuilder(di, new SolrWriter(null, null),new SimplePropertiesWriter(), new RequestInfo(null, new HashMap<String,Object>(), null));
    ContextImpl ctx = new ContextImpl(null, new VariableResolver(), null, "something", new HashMap<String,Object>(), null, db);
    String lala = new String("lala");
    ctx.setSessionAttribute("huhu", lala, Context.SCOPE_SOLR_CORE);
    Object got = ctx.getSessionAttribute("huhu", Context.SCOPE_SOLR_CORE);
    assertEquals(lala, got);
    
  }
  @Test
  public void testDocumentScope() {
    ContextImpl ctx = new ContextImpl(null, new VariableResolver(), null, "something", new HashMap<String,Object>(), null, null);
    ctx.setDoc(new DocBuilder.DocWrapper());
    String lala = new String("lala");
    ctx.setSessionAttribute("huhu", lala, Context.SCOPE_DOC);
    Object got = ctx.getSessionAttribute("huhu", Context.SCOPE_DOC);
    
    assertEquals(lala, got);
    
  }
  @Test
  public void testGlobalScope() {
    ContextImpl ctx = new ContextImpl(null, new VariableResolver(), null, "something", new HashMap<String,Object>(), null, null);
    String lala = new String("lala");
    ctx.setSessionAttribute("huhu", lala, Context.SCOPE_GLOBAL);
    Object got = ctx.getSessionAttribute("huhu", Context.SCOPE_GLOBAL);
    
    assertEquals(lala, got);
    
  }
  
}

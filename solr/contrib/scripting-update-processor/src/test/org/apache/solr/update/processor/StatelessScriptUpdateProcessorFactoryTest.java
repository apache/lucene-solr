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

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.junit.Assume;
import org.junit.BeforeClass;

/**
 * Tests {@link StatelessScriptUpdateProcessorFactory}.
 *
 * TODO: This test, to run from an IDE, requires a working directory of &lt;path-to&gt;/solr/core/src/test-files.  Fix!
 */
public class StatelessScriptUpdateProcessorFactoryTest extends UpdateProcessorTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Assume.assumeNotNull((new ScriptEngineManager()).getEngineByExtension("js"));
    initCore("solrconfig-script-updateprocessor.xml", "schema12.xml");
  }

  /**
   * simple test of a basic script processor chain using the full 
   * RequestHandler + UpdateProcessorChain flow
   */
  public void testFullRequestHandlerFlow() throws Exception {

    assertU("Simple assertion that adding a document works",
            adoc("id",  "4055",
                 "subject", "Hoss"));
    assertU(commit());

    assertQ("couldn't find hoss using script added field",
            req("q","script_added_i:[40 TO 45]",
                "fq","id:4055")
            ,"//result[@numFound=1]"
            ,"//str[@name='id'][.='4055']"
            );

    // clean up
    processDeleteById("run-no-scripts","4055");
    processCommit("run-no-scripts");
    
  }

  public void testSingleScript() throws Exception {
    SolrCore core = h.getCore();
    UpdateRequestProcessorChain chained = core.getUpdateProcessingChain("single-script");
    final StatelessScriptUpdateProcessorFactory factory = ((StatelessScriptUpdateProcessorFactory) chained.getProcessors().get(0));
    final List<String> functionMessages = new ArrayList<>();
    factory.setScriptEngineCustomizer(new ScriptEngineCustomizer() {
      @Override
      public void customize(ScriptEngine engine) {
        engine.put("functionMessages", functionMessages);
      }
    });
    assertNotNull(chained);

    SolrInputDocument d = processAdd("single-script",
        doc(f("id", "1"),
            f("name", " foo "),
            f("subject", "bar")));

    processCommit("run-no-scripts");

    assertQ("couldn't find doc by id",
            req("q","id:1")
            , "//result[@numFound=1]");

    processDeleteById("single-script","1");
    processCommit("single-script");
    
    assertQ("found deleted doc",
            req("q","id:1")
            , "//result[@numFound=0]");


    assertEquals(3, functionMessages.size());

    assertTrue(functionMessages.contains("processAdd0"));
    assertTrue(functionMessages.contains("processDelete0"));
    assertTrue(functionMessages.contains("processCommit0"));

  }

  public void testMultipleScripts() throws Exception {
    SolrCore core = h.getCore();

    for (final String chain : new String[] {"dual-scripts-arr", 
                                            "dual-scripts-strs"}) {
    
      UpdateRequestProcessorChain chained = core.getUpdateProcessingChain(chain);
      final StatelessScriptUpdateProcessorFactory factory = 
        ((StatelessScriptUpdateProcessorFactory) chained.getProcessors().get(0));
      final List<String> functionMessages = new ArrayList<>();
      ScriptEngineCustomizer customizer = new ScriptEngineCustomizer() {
          @Override
          public void customize(ScriptEngine engine) {
            engine.put("functionMessages", functionMessages);
          }
        };
      factory.setScriptEngineCustomizer(customizer);
      assertNotNull(chained);

      SolrInputDocument d = processAdd(chain,
                                       doc(f("id", "2"),
                                           f("name", " foo "),
                                           f("subject", "bar")));
      
      assertEquals(chain + " didn't add Double field", 
                   42.3d, d.getFieldValue("script_added_d"));
      assertEquals(chain + " didn't add integer field",
          42, d.getFieldValue("script_added_i"));
      
      processCommit("run-no-scripts");

      assertQ(chain + ": couldn't find doc by id",
              req("q","id:2")
              , "//result[@numFound=1]");

      processDeleteById(chain, "2");
      processCommit(chain);
      
      assertEquals(chain, 6, functionMessages.size());
      assertTrue(chain, functionMessages.contains("processAdd0"));
      assertTrue(chain, functionMessages.contains("processAdd1"));
      assertTrue(chain + ": script order doesn't match conf order",
                 functionMessages.indexOf("processAdd0") 
                 < functionMessages.indexOf("processAdd1"));

      assertTrue(chain, functionMessages.contains("processDelete0"));
      assertTrue(chain, functionMessages.contains("processDelete1"));
      assertTrue(chain + ": script order doesn't match conf order",
                 functionMessages.indexOf("processDelete0") 
                 < functionMessages.indexOf("processDelete1"));

      assertTrue(chain, functionMessages.contains("processCommit0"));
      assertTrue(chain, functionMessages.contains("processCommit1"));
      assertTrue(chain + ": script order doesn't match conf order",
                 functionMessages.indexOf("processCommit0") 
                 < functionMessages.indexOf("processCommit1"));

      finish(chain);
    
      assertEquals(chain, 8, functionMessages.size());

      assertTrue(chain, functionMessages.contains("finish0"));
      assertTrue(chain, functionMessages.contains("finish1"));
      assertTrue(chain + ": script order doesn't match conf order",
                 functionMessages.indexOf("finish0") 
                 < functionMessages.indexOf("finish1"));

      assertQ(chain + ": found deleted doc",
              req("q","id:2")
              , "//result[@numFound=0]");
      
    }
  }


  public void testConditionalExecution() throws Exception {
    for (String chain : new String[] {"conditional-script", 
                                      "conditional-scripts"}) {

      ModifiableSolrParams reqParams = new ModifiableSolrParams();
      
      SolrInputDocument d = processAdd(chain,
                                       reqParams,
                                       doc(f("id", "3"),
                                           f("name", " foo "),
                                           f("subject", "bar")));
      
      assertFalse(chain + " added String field despite condition", 
                  d.containsKey("script_added_s"));
      assertFalse(chain + " added Double field despite condition", 
                  d.containsKey("script_added_d"));
      
      reqParams.add("go-for-it", "true");
      
      d = processAdd(chain,
                     reqParams,
                     doc(f("id", "4"),
                         f("name", " foo "),
                         f("subject", "bar")));
      
      assertEquals(chain + " didn't add String field", 
                   "i went for it", d.getFieldValue("script_added_s"));
      assertEquals(chain +" didn't add Double field", 
                   42.3d, d.getFieldValue("script_added_d"));
      assertEquals(chain + " didn't add integer field",
          42, d.getFieldValue("script_added_i"));
    }
  }

  public void testForceEngine() throws Exception {
    Assume.assumeNotNull((new ScriptEngineManager()).getEngineByName("javascript"));

    final String chain = "force-script-engine";
    SolrInputDocument d = processAdd(chain,
                                     doc(f("id", "5"),
                                         f("name", " foo "),
                                         f("subject", "bar")));
      
    assertEquals(chain +" didn't add Double field", 
                 42.3d, d.getFieldValue("script_added_d"));
    assertEquals(chain + " didn't add integer field",
        42, d.getFieldValue("script_added_i"));
  }

  public void testPropogatedException() throws Exception  {
    final String chain = "error-on-add";
    SolrException e = expectThrows(SolrException.class, () ->
        processAdd(chain, doc(f("id", "5"), f("name", " foo "),
            f("subject", "bar")))
    );
    assertTrue("Exception doesn't contain script error string: " + e.getMessage(),
        0 < e.getMessage().indexOf("no-soup-fo-you"));
  }

  public void testMissingFunctions() throws Exception  {
    final String chain = "missing-functions";
    SolrException e = expectThrows(SolrException.class, () ->
        processAdd(chain, doc(f("id", "5"),
            f("name", " foo "), f("subject", "bar")))
    );
    assertTrue("Exception doesn't contain expected error: " + e.getMessage(),
        0 < e.getMessage().indexOf("processAdd"));
  }

  public void testJavaScriptCompatibility() throws Exception  {
    final String chain = "javascript-compatibility";
    SolrInputDocument d = processAdd(chain,
                                 doc(f("id", "5"),
                                     f("name", " foo "),
                                     f("subject", "BAR")));
    assertEquals("bar", d.getFieldValue("term_s"));

  }

  public void testScriptSandbox() throws Exception  {
    assumeTrue("This test only works with security manager", System.getSecurityManager() != null);
    expectThrows(SecurityException.class, () -> {
      processAdd("evil",
        doc(f("id", "5"),
            f("name", " foo "),
            f("subject", "BAR")));
    });
  }

}

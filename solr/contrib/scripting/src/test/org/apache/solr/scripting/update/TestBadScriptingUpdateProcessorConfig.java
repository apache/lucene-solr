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
package org.apache.solr.scripting.update;

import java.util.Map;
import java.util.regex.Pattern;

import javax.script.ScriptEngineManager;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.CoreContainer;
import org.junit.Assume;

public class TestBadScriptingUpdateProcessorConfig extends SolrTestCaseJ4 {


  public void testBogusScriptEngine() throws Exception {
    // sanity check
    Assume.assumeTrue(null == (new ScriptEngineManager()).getEngineByName("giberish"));

    assertConfigs("bad-solrconfig-bogus-scriptengine-name.xml",
                  "schema.xml",getFile("scripting/solr/collection1").getParent(),"giberish");
  }

  public void testMissingScriptFile() throws Exception {
    // sanity check
    Assume.assumeNotNull((new ScriptEngineManager()).getEngineByExtension("js"));
    assertConfigs("bad-solrconfig-missing-scriptfile.xml",
                  "schema.xml",getFile("scripting/solr/collection1").getParent(),"a-file-name-that-does-not-exist.js");
  }

  public void testInvalidScriptFile() throws Exception {
    // sanity check
    Assume.assumeNotNull((new ScriptEngineManager()).getEngineByName("javascript"));
    assertConfigs("bad-solrconfig-invalid-scriptfile.xml",
                  "schema.xml",getFile("scripting/solr/collection1").getParent(),"invalid.script.xml");
  }
  
    /**
     * Given a solrconfig.xml file name, a schema file name, a solr home directory, 
     * and an expected errString, asserts that initializing a core with these 
     * files causes an error matching the specified errString ot be thrown.
     */
  protected final void assertConfigs(final String solrconfigFile,
                                     final String schemaFile,
                                     final String solrHome,
                                     final String errString) 
    throws Exception {

    ignoreException(Pattern.quote(errString));
    try {

      if (null == solrHome) {
        initCore( solrconfigFile, schemaFile );
      } else {
        initCore( solrconfigFile, schemaFile, solrHome );
      }

      CoreContainer cc = h.getCoreContainer();
      for (Map.Entry<String, CoreContainer.CoreLoadFailure> entry : cc.getCoreInitFailures().entrySet()) {
        if (matches(entry.getValue().exception, errString))
          return;
      }
    }
    catch (Exception e) {
      if (matches(e, errString))
        return;
      throw e;
    }
    finally {
      deleteCore();
      resetExceptionIgnores();
    }
    fail("Did not encounter any exception from: " + solrconfigFile + " using " + schemaFile);
  }

  private static boolean matches(Exception e, String errString) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t.getMessage() != null && -1 != t.getMessage().indexOf(errString))
        return true;
    }
    return false;
  }  

}

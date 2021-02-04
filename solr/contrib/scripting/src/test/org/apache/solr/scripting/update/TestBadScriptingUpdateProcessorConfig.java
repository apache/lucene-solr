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

import javax.script.ScriptEngineManager;

import org.apache.solr.core.AbstractBadConfigTestBase;
import org.junit.Assume;

public class TestBadScriptingUpdateProcessorConfig extends AbstractBadConfigTestBase {


  public void testBogusScriptEngine() throws Exception {
    // sanity check
    Assume.assumeTrue(null == (new ScriptEngineManager()).getEngineByName("giberish"));

    assertConfigs("bad-solrconfig-bogus-scriptengine-name.xml",
                  "schema.xml","giberish");
  }

  public void testMissingScriptFile() throws Exception {
    // sanity check
    Assume.assumeNotNull((new ScriptEngineManager()).getEngineByExtension("js"));
    assertConfigs("bad-solrconfig-missing-scriptfile.xml",
                  "schema.xml","a-file-name-that-does-not-exist.js");
  }

  public void testInvalidScriptFile() throws Exception {
    // sanity check
    Assume.assumeNotNull((new ScriptEngineManager()).getEngineByName("javascript"));
    assertConfigs("bad-solrconfig-invalid-scriptfile.xml",
                  "schema.xml","invalid.script.xml");
  }

}

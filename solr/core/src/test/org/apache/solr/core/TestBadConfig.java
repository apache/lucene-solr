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
package org.apache.solr.core;

import javax.script.ScriptEngineManager;

import org.junit.Assume;
import org.junit.Ignore;

public class TestBadConfig extends AbstractBadConfigTestBase {

  public void testUnsetSysProperty() throws Exception {
    assertConfigs("bad_solrconfig.xml","schema.xml","unset.sys.property");
  }

  public void testNRTModeProperty() throws Exception {
    assertConfigs("bad-solrconfig-nrtmode.xml","schema.xml", "nrtMode");
  }



@Ignore
public void testMultipleDirectoryFactories() throws Exception {
      assertConfigs("bad-solrconfig-multiple-dirfactory.xml", "schema12.xml",
                    "directoryFactory");
  }
  @Ignore
  public void testMultipleIndexConfigs() throws Exception {
      assertConfigs("bad-solrconfig-multiple-indexconfigs.xml", "schema12.xml",
                    "indexConfig");
  }
  @Ignore
  public void testMultipleCFS() throws Exception {
      assertConfigs("bad-solrconfig-multiple-cfs.xml", "schema12.xml",
                    "useCompoundFile");
  }

  public void testUpdateLogButNoVersionField() throws Exception {
    
    System.setProperty("enable.update.log", "true");
    try {
      assertConfigs("solrconfig.xml", "schema12.xml", "_version_");
    } finally {
      System.clearProperty("enable.update.log");
    }
  }

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
                  "schema.xml","currency.xml");
  }

  public void testBogusMergePolicy() throws Exception {
    assertConfigs("bad-mpf-solrconfig.xml", "schema-minimal.xml",
                  "DummyMergePolicyFactory");
  }

  public void testSchemaMutableButNotManaged() throws Exception {
    assertConfigs("bad-solrconfig-schema-mutable-but-not-managed.xml",
                  "schema-minimal.xml", "Unexpected arg(s): {mutable=false,managedSchemaResourceName=schema.xml}");
  }

  public void testManagedSchemaCannotBeNamedSchemaDotXml() throws Exception {
    assertConfigs("bad-solrconfig-managed-schema-named-schema.xml.xml",
                  "schema-minimal.xml", "managedSchemaResourceName can't be 'schema.xml'");
  }
  
  public void testUnknownSchemaAttribute() throws Exception {
    assertConfigs("bad-solrconfig-unexpected-schema-attribute.xml", "schema-minimal.xml",
                  "Unexpected arg(s): {bogusParam=bogusValue}");
  }

  public void testTolerantUpdateProcessorNoUniqueKey() throws Exception {
    assertConfigs("solrconfig-tolerant-update-minimal.xml", "schema-minimal.xml",
                  "requires a schema that includes a uniqueKey field");
  }
}

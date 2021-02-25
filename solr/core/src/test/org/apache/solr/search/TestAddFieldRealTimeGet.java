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
package org.apache.solr.search;

import java.io.File;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.junit.Before;

public class TestAddFieldRealTimeGet extends TestRTGBase {

  private static File tmpSolrHome;
  private static File tmpConfDir;

  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";

  @Before
  private void initManagedSchemaCore() throws Exception {
    final String tmpSolrHomePath = createTempDir().toFile().getAbsolutePath();
    tmpSolrHome = new File(tmpSolrHomePath).getAbsoluteFile();
    tmpConfDir = new File(tmpSolrHome, confDir);
    File testHomeConfDir = new File(TEST_HOME(), confDir);
    final String configFileName = "solrconfig-managed-schema.xml";
    final String schemaFileName = "schema-id-and-version-fields-only.xml";
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, configFileName), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, schemaFileName), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "solrconfig.snippet.randomindexconfig.xml"), tmpConfDir);

    // initCore will trigger an upgrade to managed schema, since the solrconfig has
    // <schemaFactory class="ManagedIndexSchemaFactory" ... />
    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "true");
    initCore(configFileName, schemaFileName, tmpSolrHome.getPath());
  }

  public void test() throws Exception {
    clearIndex();
    assertU(commit());

    String newFieldName = "newfield";
    String newFieldType = "string";
    String newFieldValue = "xyz";

    ignoreException("unknown field");
    assertFailedU("Should fail due to unknown field '" + newFieldName + "'", 
                  adoc("id", "1", newFieldName, newFieldValue));
    unIgnoreException("unknown field");

    IndexSchema schema = h.getCore().getLatestSchema();
    SchemaField newField = schema.newField(newFieldName, newFieldType, Collections.emptyMap());
    IndexSchema newSchema = schema.addField(newField);
    h.getCore().setLatestSchema(newSchema);
    
    String newFieldKeyValue = "'" + newFieldName + "':'" + newFieldValue + "'"; 
    assertU(adoc("id", "1", newFieldName, newFieldValue));
    assertJQ(req("q","id:1"), 
             "/response/numFound==0");
    assertJQ(req("qt","/get", "id","1", "fl","id,"+newFieldName),
             "=={'doc':{'id':'1'," + newFieldKeyValue + "}}");
    assertJQ(req("qt","/get","ids","1", "fl","id,"+newFieldName),
             "=={'response':{'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'1'," + newFieldKeyValue + "}]}}");

    assertU(commit());

    assertJQ(req("q","id:1"), 
             "/response/numFound==1");
    assertJQ(req("qt","/get", "id","1", "fl","id,"+newFieldName),
        "=={'doc':{'id':'1'," + newFieldKeyValue + "}}");
    assertJQ(req("qt","/get","ids","1", "fl","id,"+newFieldName),
        "=={'response':{'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'1'," + newFieldKeyValue + "}]}}");
  }
}

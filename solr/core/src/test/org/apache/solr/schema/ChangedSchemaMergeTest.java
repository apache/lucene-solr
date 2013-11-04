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

package org.apache.solr.schema;

import org.apache.commons.codec.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.UpdateHandler;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ChangedSchemaMergeTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore();
  }

  private final File solrHomeDirectory = new File(TEMP_DIR, getSimpleClassName());
  private File schemaFile = null;

  private void addDoc(SolrCore core, String... fieldValues) throws IOException {
    UpdateHandler updater = core.getUpdateHandler();
    AddUpdateCommand cmd = new AddUpdateCommand(new LocalSolrQueryRequest(core, new NamedList<Object>()));
    cmd.solrDoc = sdoc((Object[]) fieldValues);
    updater.addDoc(cmd);
  }

  private CoreContainer init() throws Exception {

    if (solrHomeDirectory.exists()) {
      FileUtils.deleteDirectory(solrHomeDirectory);
    }
    assertTrue("Failed to mkdirs workDir", solrHomeDirectory.mkdirs());
    File changed = new File(solrHomeDirectory, "changed");
    copyMinConf(changed, "name=changed");
    // Overlay with my local schema
    schemaFile = new File(new File(changed, "conf"), "schema.xml");
    FileUtils.writeStringToFile(schemaFile, withWhich, Charsets.UTF_8.toString());

    String discoveryXml = "<solr></solr>";
    File solrXml = new File(solrHomeDirectory, "solr.xml");
    FileUtils.write(solrXml, discoveryXml, Charsets.UTF_8.toString());

    final CoreContainer cores = new CoreContainer(solrHomeDirectory.getAbsolutePath());
    cores.load();
    return cores;
  }

  @Test
  public void testOptimizeDiffSchemas() throws Exception {
    // load up a core (why not put it on disk?)
    CoreContainer cc = init();
    SolrCore changed = cc.getCore("changed");
    try {

      // add some documents
      addDoc(changed, "id", "1", "which", "15", "text", "some stuff with which");
      addDoc(changed, "id", "2", "which", "15", "text", "some stuff with which");
      addDoc(changed, "id", "3", "which", "15", "text", "some stuff with which");
      addDoc(changed, "id", "4", "which", "15", "text", "some stuff with which");
      SolrQueryRequest req = new LocalSolrQueryRequest(changed, new NamedList<Object>());
      changed.getUpdateHandler().commit(new CommitUpdateCommand(req, false));

      // write the new schema out and make it current
      FileUtils.writeStringToFile(schemaFile, withoutWhich, Charsets.UTF_8.toString());

      IndexSchema iSchema = IndexSchemaFactory.buildIndexSchema("schema.xml", changed.getSolrConfig());
      changed.setLatestSchema(iSchema);

      addDoc(changed, "id", "1", "text", "some stuff without which");
      addDoc(changed, "id", "5", "text", "some stuff without which");

      changed.getUpdateHandler().commit(new CommitUpdateCommand(req, false));
      changed.getUpdateHandler().commit(new CommitUpdateCommand(req, true));
    } finally {
      if (changed != null) changed.close();
      if (cc != null) cc.shutdown();
    }
  }


  private static String withWhich = "<schema name=\"tiny\" version=\"1.1\">\n" +
      "  <fields>\n" +
      "    <field name=\"id\" type=\"string\" indexed=\"true\" stored=\"true\" required=\"true\"/>\n" +
      "    <field name=\"text\" type=\"text\" indexed=\"true\" stored=\"true\"/>\n" +
      "    <field name=\"which\" type=\"int\" indexed=\"true\" stored=\"true\"/>\n" +
      "  </fields>\n" +
      "  <uniqueKey>id</uniqueKey>\n" +
      "\n" +
      "  <types>\n" +
      "    <fieldtype name=\"text\" class=\"solr.TextField\">\n" +
      "      <analyzer>\n" +
      "        <tokenizer class=\"solr.WhitespaceTokenizerFactory\"/>\n" +
      "        <filter class=\"solr.LowerCaseFilterFactory\"/>\n" +
      "      </analyzer>\n" +

      "    </fieldtype>\n" +
      "    <fieldType name=\"string\" class=\"solr.StrField\"/>\n" +
      "    <fieldType name=\"int\" class=\"solr.TrieIntField\" precisionStep=\"0\" positionIncrementGap=\"0\"/>" +
      "  </types>\n" +
      "</schema>";

  private static String withoutWhich = "<schema name=\"tiny\" version=\"1.1\">\n" +
      "  <fields>\n" +
      "    <field name=\"id\" type=\"string\" indexed=\"true\" stored=\"true\" required=\"true\"/>\n" +
      "    <field name=\"text\" type=\"text\" indexed=\"true\" stored=\"true\"/>\n" +
      "  </fields>\n" +
      "  <uniqueKey>id</uniqueKey>\n" +
      "\n" +
      "  <types>\n" +
      "    <fieldtype name=\"text\" class=\"solr.TextField\">\n" +
      "      <analyzer>\n" +
      "        <tokenizer class=\"solr.WhitespaceTokenizerFactory\"/>\n" +
      "        <filter class=\"solr.LowerCaseFilterFactory\"/>\n" +
      "      </analyzer>\n" +
      "    </fieldtype>\n" +
      "    <fieldType name=\"string\" class=\"solr.StrField\"/>\n" +
      "    <fieldType name=\"int\" class=\"solr.TrieIntField\" precisionStep=\"0\" positionIncrementGap=\"0\"/>" +
      "  </types>\n" +
      "</schema>";


}

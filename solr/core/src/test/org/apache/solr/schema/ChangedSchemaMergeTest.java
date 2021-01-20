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

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.similarities.LMJelinekMercerSimilarityFactory;
import org.apache.solr.search.similarities.SchemaSimilarityFactory;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangedSchemaMergeTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static Class<? extends SimilarityFactory> simfac1;
  public static Class<? extends SimilarityFactory> simfac2;
  
  @BeforeClass
  public static void beforeClass() throws Exception {

    simfac1 = LMJelinekMercerSimilarityFactory.class;
    simfac2 = SchemaSimilarityFactory.class;
    
    // sanity check our test...
    assertTrue("Effectiveness of tets depends on SchemaSimilarityFactory being SolrCoreAware " + 
               "something changed in the impl and now major portions of this test are useless",
               SolrCoreAware.class.isAssignableFrom(simfac2));
    
    // randomize the order these similarities are used in the changed schemas
    // to help test proper initialization in both code paths
    if (random().nextBoolean()) {
      Class<? extends SimilarityFactory> tmp = simfac1;
      simfac1 = simfac2;
      simfac2 = tmp;
    }
    System.setProperty("solr.test.simfac1", simfac1.getName());
    System.setProperty("solr.test.simfac2", simfac2.getName());
      
    initCore();
  }

  private final File solrHomeDirectory = createTempDir().toFile();
  private File schemaFile = null;

  private void addDoc(SolrCore core, String... fieldValues) throws IOException {
    UpdateHandler updater = core.getUpdateHandler();
    AddUpdateCommand cmd = new AddUpdateCommand(new LocalSolrQueryRequest(core, new NamedList<>()));
    cmd.solrDoc = sdoc((Object[]) fieldValues);
    updater.addDoc(cmd);
  }

  private CoreContainer init() throws Exception {
    File changed = new File(solrHomeDirectory, "changed");
    copyMinConf(changed, "name=changed");
    // Overlay with my local schema
    schemaFile = new File(new File(changed, "conf"), "schema.xml");
    FileUtils.writeStringToFile(schemaFile, withWhich, StandardCharsets.UTF_8);

    String discoveryXml = "<solr></solr>";
    File solrXml = new File(solrHomeDirectory, "solr.xml");
    FileUtils.write(solrXml, discoveryXml, StandardCharsets.UTF_8);

    final CoreContainer cores = new CoreContainer(solrHomeDirectory.toPath(), new Properties());
    cores.load();
    return cores;
  }

  public void testSanityOfSchemaSimilarityFactoryInform() {
    // sanity check that SchemaSimilarityFactory will throw an Exception if you
    // try to use it w/o inform(SolrCoreAware) otherwise assertSimilarity is useless
    SchemaSimilarityFactory broken = new SchemaSimilarityFactory();
    broken.init(new ModifiableSolrParams());
    // NO INFORM
    IllegalStateException e = expectThrows(IllegalStateException.class, broken::getSimilarity);
    assertTrue("GOT: " + e.getMessage(),
        e.getMessage().contains("SolrCoreAware.inform"));
  }
  
  @Test
  public void testOptimizeDiffSchemas() throws Exception {
    // load up a core (why not put it on disk?)
    CoreContainer cc = init();
    try (SolrCore changed = cc.getCore("changed")) {

      assertSimilarity(changed, simfac1);
                       
      // add some documents
      addDoc(changed, "id", "1", "which", "15", "text", "some stuff with which");
      addDoc(changed, "id", "2", "which", "15", "text", "some stuff with which");
      addDoc(changed, "id", "3", "which", "15", "text", "some stuff with which");
      addDoc(changed, "id", "4", "which", "15", "text", "some stuff with which");
      SolrQueryRequest req = new LocalSolrQueryRequest(changed, new NamedList<>());
      changed.getUpdateHandler().commit(new CommitUpdateCommand(req, false));

      // write the new schema out and make it current
      FileUtils.writeStringToFile(schemaFile, withoutWhich, StandardCharsets.UTF_8);

      IndexSchema iSchema = IndexSchemaFactory.buildIndexSchema("schema.xml", changed.getSolrConfig());
      changed.setLatestSchema(iSchema);
      
      assertSimilarity(changed, simfac2);
      // sanity check our sanity check
      assertFalse("test is broken: both simfacs are the same", simfac1.equals(simfac2)); 

      addDoc(changed, "id", "1", "text", "some stuff without which");
      addDoc(changed, "id", "5", "text", "some stuff without which");

      changed.getUpdateHandler().commit(new CommitUpdateCommand(req, false));
      changed.getUpdateHandler().commit(new CommitUpdateCommand(req, true));
    } catch (Throwable e) {
      log.error("Test exception, logging so not swallowed if there is a (finally) shutdown exception: "
          , e);
      throw e;
    } finally {
      if (cc != null) cc.shutdown();
    }
  }

  private static void assertSimilarity(SolrCore core, Class<? extends SimilarityFactory> expected) {
    SimilarityFactory actual = core.getLatestSchema().getSimilarityFactory();
    assertNotNull(actual);
    assertEquals(expected, actual.getClass());
    // if SolrCoreAware sim isn't properly initialized, this will throw an exception
    assertNotNull(actual.getSimilarity());
  }

  private String withWhich = "<schema name=\"tiny\" version=\"1.1\">\n" +
      "    <field name=\"id\" type=\"string\" indexed=\"true\" stored=\"true\" required=\"true\"/>\n" +
      "    <field name=\"text\" type=\"text\" indexed=\"true\" stored=\"true\"/>\n" +
      "    <field name=\"which\" type=\"int\" indexed=\"true\" stored=\"true\"/>\n" +
      "  <uniqueKey>id</uniqueKey>\n" +
      "\n" +
      "    <fieldtype name=\"text\" class=\"solr.TextField\">\n" +
      "      <analyzer>\n" +
      "        <tokenizer class=\"solr.WhitespaceTokenizerFactory\"/>\n" +
      "        <filter class=\"solr.LowerCaseFilterFactory\"/>\n" +
      "      </analyzer>\n" +

      "    </fieldtype>\n" +
      "    <fieldType name=\"string\" class=\"solr.StrField\"/>\n" +
      "    <fieldType name=\"int\" class=\""+RANDOMIZED_NUMERIC_FIELDTYPES.get(Integer.class)+"\" precisionStep=\"0\" positionIncrementGap=\"0\"/>" +
      "  <similarity class=\"${solr.test.simfac1}\"/> " +
      "</schema>";

  private String withoutWhich = "<schema name=\"tiny\" version=\"1.1\">\n" +
      "    <field name=\"id\" type=\"string\" indexed=\"true\" stored=\"true\" required=\"true\"/>\n" +
      "    <field name=\"text\" type=\"text\" indexed=\"true\" stored=\"true\"/>\n" +
      "  <uniqueKey>id</uniqueKey>\n" +
      "\n" +
      "    <fieldtype name=\"text\" class=\"solr.TextField\">\n" +
      "      <analyzer>\n" +
      "        <tokenizer class=\"solr.WhitespaceTokenizerFactory\"/>\n" +
      "        <filter class=\"solr.LowerCaseFilterFactory\"/>\n" +
      "      </analyzer>\n" +
      "    </fieldtype>\n" +
      "    <fieldType name=\"string\" class=\"solr.StrField\"/>\n" +
      "    <fieldType name=\"int\" class=\""+RANDOMIZED_NUMERIC_FIELDTYPES.get(Integer.class)+"\" precisionStep=\"0\" positionIncrementGap=\"0\"/>" +
      "  <similarity class=\"${solr.test.simfac2}\"/> " +
      "</schema>";


}

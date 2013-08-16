package org.apache.solr.core;

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

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSolrXmlPersistor {

  private static final List<CoreDescriptor> EMPTY_CD_LIST = ImmutableList.<CoreDescriptor>builder().build();

  @Test
  public void selfClosingCoresTagIsPersisted() {

    final String solrxml = "<solr><cores adminHandler=\"/admin\"/></solr>";

    SolrXMLCoresLocator persistor = new SolrXMLCoresLocator(solrxml, null);
    assertEquals(persistor.buildSolrXML(EMPTY_CD_LIST),
        "<solr><cores adminHandler=\"/admin\"></cores></solr>");

  }

  @Test
  public void emptyCoresTagIsPersisted() {
    final String solrxml = "<solr><cores adminHandler=\"/admin\"></cores></solr>";

    SolrXMLCoresLocator persistor = new SolrXMLCoresLocator(solrxml, null);
    assertEquals(persistor.buildSolrXML(EMPTY_CD_LIST), "<solr><cores adminHandler=\"/admin\"></cores></solr>");
  }

  @Test
  public void emptySolrXmlIsPersisted() {
    final String solrxml = "<solr></solr>";

    SolrXMLCoresLocator persistor = new SolrXMLCoresLocator(solrxml, null);
    assertEquals(persistor.buildSolrXML(EMPTY_CD_LIST), "<solr><cores></cores></solr>");
  }

  @Test
  public void simpleCoreDescriptorIsPersisted() {

    final String solrxml = "<solr><cores></cores></solr>";

    SolrResourceLoader loader = new SolrResourceLoader("solr/example/solr");
    CoreContainer cc = new CoreContainer(loader);

    final CoreDescriptor cd = new CoreDescriptor(cc, "testcore", "instance/dir/");
    List<CoreDescriptor> cds = ImmutableList.of(cd);

    SolrXMLCoresLocator persistor = new SolrXMLCoresLocator(solrxml, null);
    assertEquals(persistor.buildSolrXML(cds),
          "<solr><cores>" + SolrXMLCoresLocator.NEWLINE
        + "    <core name=\"testcore\" instanceDir=\"instance/dir/\"/>" + SolrXMLCoresLocator.NEWLINE
        + "</cores></solr>");
  }

  @Test
  public void shardHandlerInfoIsPersisted() {

    final String solrxml =
        "<solr>" +
          "<cores adminHandler=\"whatever\">" +
            "<core name=\"testcore\" instanceDir=\"instance/dir/\"/>" +
            "<shardHandlerFactory name=\"shardHandlerFactory\" class=\"HttpShardHandlerFactory\">" +
              "<int name=\"socketTimeout\">${socketTimeout:500}</int>" +
              "<str name=\"arbitrary\">arbitraryValue</str>" +
            "</shardHandlerFactory>" +
          "</cores>" +
        "</solr>";

    SolrXMLCoresLocator locator = new SolrXMLCoresLocator(solrxml, null);
    assertTrue(locator.getTemplate().contains("{{CORES_PLACEHOLDER}}"));
    assertTrue(locator.getTemplate().contains("<shardHandlerFactory "));
    assertTrue(locator.getTemplate().contains("${socketTimeout:500}"));

  }

  @Test
  public void simpleShardHandlerInfoIsPersisted() {

    final String solrxml =
        "<solr>" +
          "<cores adminHandler=\"whatever\">" +
            "<core name=\"testcore\" instanceDir=\"instance/dir/\"/>" +
            "<shardHandlerFactory name=\"shardHandlerFactory\" class=\"HttpShardHandlerFactory\"/>" +
          "</cores>" +
        "</solr>";

    SolrXMLCoresLocator locator = new SolrXMLCoresLocator(solrxml, null);
    assertTrue(locator.getTemplate().contains("{{CORES_PLACEHOLDER}}"));
    assertTrue(locator.getTemplate().contains("<shardHandlerFactory "));
  }

  @Test
  public void complexXmlIsParsed() {
    SolrXMLCoresLocator locator = new SolrXMLCoresLocator(TestSolrXmlPersistence.SOLR_XML_LOTS_SYSVARS, null);
    assertTrue(locator.getTemplate().contains("{{CORES_PLACEHOLDER}}"));
  }

}

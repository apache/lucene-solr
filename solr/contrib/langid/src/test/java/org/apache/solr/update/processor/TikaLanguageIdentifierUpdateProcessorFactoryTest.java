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

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;

public class TikaLanguageIdentifierUpdateProcessorFactoryTest extends LanguageIdentifierUpdateProcessorFactoryTestCase {
  @Override
  protected LanguageIdentifierUpdateProcessor createLangIdProcessor(ModifiableSolrParams parameters) throws Exception {
    return new TikaLanguageIdentifierUpdateProcessor(_parser.buildRequestFrom(h.getCore(), parameters, null), resp, null);
  }


  @Test
  public void testMaxFieldValueChars() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    String valueF1 = "Apache Lucene is a free/open source information retrieval software library, originally created in Java by Doug Cutting. It is supported by the Apache Software Foundation and is released under the Apache Software License.";
    String valueF2 = "An open-source search server based on the Lucene Java search library. News, documentation, resources, and download.";
    doc.addField("foo_s", valueF1);

    ModifiableSolrParams parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "foo_s");
    parameters.add("langid.langField", "language");
    parameters.add("langid.enforceSchema", "false");
    TikaLanguageIdentifierUpdateProcessor p = (TikaLanguageIdentifierUpdateProcessor) createLangIdProcessor(parameters);
    assertEquals(valueF1, p.concatFields(doc).trim());

    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "foo_s");
    parameters.add("langid.langField", "language");
    parameters.add("langid.enforceSchema", "false");
    parameters.add("langid.maxFieldValueChars", "6");
    p = (TikaLanguageIdentifierUpdateProcessor) createLangIdProcessor(parameters);
    assertEquals("Apache", p.concatFields(doc).trim());

    doc.addField("bar_s", valueF2);

    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "foo_s,bar_s");
    parameters.add("langid.langField", "language");
    parameters.add("langid.enforceSchema", "false");
    p = (TikaLanguageIdentifierUpdateProcessor) createLangIdProcessor(parameters);
    assertEquals(valueF1 + " " + valueF2, p.concatFields(doc).trim());

    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "foo_s,bar_s");
    parameters.add("langid.langField", "language");
    parameters.add("langid.enforceSchema", "false");
    parameters.add("langid.maxFieldValueChars", "6");
    p = (TikaLanguageIdentifierUpdateProcessor) createLangIdProcessor(parameters);
    assertEquals("Apache" + " " + "An ope", p.concatFields(doc).trim());

    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "foo_s,bar_s");
    parameters.add("langid.langField", "language");
    parameters.add("langid.enforceSchema", "false");
    parameters.add("langid.maxFieldValueChars", "100000");
    p = (TikaLanguageIdentifierUpdateProcessor) createLangIdProcessor(parameters);
    assertEquals(valueF1 + " " + valueF2, p.concatFields(doc).trim());

}

  @Test
  public void testMaxTotalChars() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    String valueF1 = "Apache Lucene is a free/open source information retrieval software library, originally created in Java by Doug Cutting. It is supported by the Apache Software Foundation and is released under the Apache Software License.";
    String valueF2 = "An open-source search server based on the Lucene Java search library. News, documentation, resources, and download.";
    doc.addField("foo_s", valueF1);

    ModifiableSolrParams parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "foo_s");
    parameters.add("langid.langField", "language");
    parameters.add("langid.enforceSchema", "false");
    TikaLanguageIdentifierUpdateProcessor p = (TikaLanguageIdentifierUpdateProcessor) createLangIdProcessor(parameters);
    assertEquals(valueF1, p.concatFields(doc).trim());

    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "foo_s");
    parameters.add("langid.langField", "language");
    parameters.add("langid.enforceSchema", "false");
    parameters.add("langid.maxTotalChars", "6");
    p = (TikaLanguageIdentifierUpdateProcessor) createLangIdProcessor(parameters);
    assertEquals("Apache", p.concatFields(doc).trim());

    doc.addField("bar_s", valueF2);

    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "foo_s,bar_s");
    parameters.add("langid.langField", "language");
    parameters.add("langid.enforceSchema", "false");
    p = (TikaLanguageIdentifierUpdateProcessor) createLangIdProcessor(parameters);
    assertEquals(valueF1 + " " + valueF2, p.concatFields(doc).trim());

    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "foo_s,bar_s");
    parameters.add("langid.langField", "language");
    parameters.add("langid.enforceSchema", "false");
    parameters.add("langid.maxTotalChars", "6");
    p = (TikaLanguageIdentifierUpdateProcessor) createLangIdProcessor(parameters);
    assertEquals("Apache", p.concatFields(doc).trim());

    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "foo_s,bar_s");
    parameters.add("langid.langField", "language");
    parameters.add("langid.enforceSchema", "false");
    parameters.add("langid.maxTotalChars", "100000");
    p = (TikaLanguageIdentifierUpdateProcessor) createLangIdProcessor(parameters);
    assertEquals(valueF1 + " " + valueF2, p.concatFields(doc).trim());

  }


  @Test
  public void testMaxFieldValueCharsAndMaxTotalChars() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    String valueF1 = "Apache Lucene is a free/open source information retrieval software library, originally created in Java by Doug Cutting. It is supported by the Apache Software Foundation and is released under the Apache Software License.";
    String valueF2 = "An open-source search server based on the Lucene Java search library. News, documentation, resources, and download.";
    doc.addField("foo_s", valueF1);

    ModifiableSolrParams parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "foo_s");
    parameters.add("langid.langField", "language");
    parameters.add("langid.enforceSchema", "false");
    TikaLanguageIdentifierUpdateProcessor p = (TikaLanguageIdentifierUpdateProcessor) createLangIdProcessor(parameters);
    assertEquals(valueF1, p.concatFields(doc).trim());

    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "foo_s");
    parameters.add("langid.langField", "language");
    parameters.add("langid.enforceSchema", "false");
    parameters.add("langid.maxFieldValueChars", "8");
    parameters.add("langid.maxTotalChars", "6");
    p = (TikaLanguageIdentifierUpdateProcessor) createLangIdProcessor(parameters);
    assertEquals("Apache", p.concatFields(doc).trim());

    doc.addField("bar_s", valueF2);

    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "foo_s,bar_s");
    parameters.add("langid.langField", "language");
    parameters.add("langid.enforceSchema", "false");
    p = (TikaLanguageIdentifierUpdateProcessor) createLangIdProcessor(parameters);
    assertEquals(valueF1 + " " + valueF2, p.concatFields(doc).trim());

    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "foo_s,bar_s");
    parameters.add("langid.langField", "language");
    parameters.add("langid.enforceSchema", "false");
    parameters.add("langid.maxFieldValueChars", "3");
    parameters.add("langid.maxTotalChars", "8");
    p = (TikaLanguageIdentifierUpdateProcessor) createLangIdProcessor(parameters);
    assertEquals("Apa An", p.concatFields(doc).trim());

    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "foo_s,bar_s");
    parameters.add("langid.langField", "language");
    parameters.add("langid.enforceSchema", "false");
    parameters.add("langid.maxFieldValueChars", "10000");
    parameters.add("langid.maxTotalChars", "100000");
    p = (TikaLanguageIdentifierUpdateProcessor) createLangIdProcessor(parameters);
    assertEquals(valueF1 + " " + valueF2, p.concatFields(doc).trim());

  }

}

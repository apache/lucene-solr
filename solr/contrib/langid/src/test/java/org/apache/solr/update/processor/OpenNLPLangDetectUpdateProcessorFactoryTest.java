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
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Test;

public class OpenNLPLangDetectUpdateProcessorFactoryTest extends LanguageIdentifierUpdateProcessorFactoryTestCase {
  private static final String TEST_MODEL = "opennlp-langdetect.eng-swe-spa-rus-deu.bin";
  
  @Override
  protected OpenNLPLangDetectUpdateProcessor createLangIdProcessor(ModifiableSolrParams parameters) throws Exception {
    if (parameters.get("langid.model") == null) { // handle superclass tests that don't provide the model filename
      parameters.set("langid.model", TEST_MODEL);
    }
    if (parameters.get("langid.threshold") == null) { // handle superclass tests that don't provide confidence threshold
      parameters.set("langid.threshold", "0.3");
    }
    SolrQueryRequest req = _parser.buildRequestFrom(h.getCore(), new ModifiableSolrParams(), null);
    OpenNLPLangDetectUpdateProcessorFactory factory = new OpenNLPLangDetectUpdateProcessorFactory();
    factory.init(parameters.toNamedList());
    factory.inform(h.getCore());
    return (OpenNLPLangDetectUpdateProcessor)factory.getInstance(req, resp, null);
  }

  // this one actually works better it seems with short docs
  @Override
  protected SolrInputDocument tooShortDoc() {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("text", "");
    return doc;
  }

  @Test @Override
  public void testLangIdGlobal() throws Exception {
    ModifiableSolrParams parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "name,subject");
    parameters.add("langid.langField", "language_s");
    parameters.add("langid.model", TEST_MODEL);
    parameters.add("langid.threshold", "0.3");
    liProcessor = createLangIdProcessor(parameters);

    assertLang("en", "id", "1en", "name", "Lucene", "subject", "Apache Lucene is a free/open source information retrieval software library, originally created in Java by Doug Cutting. It is supported by the Apache Software Foundation and is released under the Apache Software License.");
    assertLang("sv", "id", "2sv", "name", "Maven", "subject", "Apache Maven är ett verktyg utvecklat av Apache Software Foundation och används inom systemutveckling av datorprogram i programspråket Java. Maven används för att automatiskt paketera (bygga) programfilerna till en distribuerbar enhet. Maven används inom samma område som Apache Ant men dess byggfiler är deklarativa till skillnad ifrån Ants skriptbaserade.");
    assertLang("es", "id", "3es", "name", "Lucene", "subject", "Lucene es un API de código abierto para recuperación de información, originalmente implementada en Java por Doug Cutting. Está apoyado por el Apache Software Foundation y se distribuye bajo la Apache Software License. Lucene tiene versiones para otros lenguajes incluyendo Delphi, Perl, C#, C++, Python, Ruby y PHP.");
    assertLang("ru", "id", "4ru", "name", "Lucene", "subject", "The Apache Lucene — это свободная библиотека для высокоскоростного полнотекстового поиска, написанная на Java. Может быть использована для поиска в интернете и других областях компьютерной лингвистики (аналитическая философия).");
    assertLang("de", "id", "5de", "name", "Lucene", "subject", "Lucene ist ein Freie-Software-Projekt der Apache Software Foundation, das eine Suchsoftware erstellt. Durch die hohe Leistungsfähigkeit und Skalierbarkeit können die Lucene-Werkzeuge für beliebige Projektgrößen und Anforderungen eingesetzt werden. So setzt beispielsweise Wikipedia Lucene für die Volltextsuche ein. Zudem verwenden die beiden Desktop-Suchprogramme Beagle und Strigi eine C#- bzw. C++- Portierung von Lucene als Indexer.");
  }
}

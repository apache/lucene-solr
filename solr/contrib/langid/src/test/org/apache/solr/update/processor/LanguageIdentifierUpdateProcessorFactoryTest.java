/**
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

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.SolrRequestParsers;

public class LanguageIdentifierUpdateProcessorFactoryTest extends SolrTestCaseJ4 {

  protected static SolrRequestParsers _parser;
  protected static SolrQueryRequest req;
  protected static SolrQueryResponse resp = new SolrQueryResponse();
  protected static LanguageIdentifierUpdateProcessor liProcessor;
  protected static ModifiableSolrParams parameters;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-languageidentifier.xml", "schema.xml", getFile("langid/solr").getAbsolutePath());
    SolrCore core = h.getCore();
    UpdateRequestProcessorChain chained = core.getUpdateProcessingChain("lang_id");
    assertNotNull(chained);
    _parser = new SolrRequestParsers(null);
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  @Test
  public void testLangIdGlobal() throws Exception {
    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "name,subject");
    parameters.add("langid.langField", "language_s");
    parameters.add("langid.fallback", "un");
    liProcessor = createLangIdProcessor(parameters);
    
    assertLang("no", "id", "1no", "name", "Lucene", "subject", "Lucene er et fri/åpen kildekode programvarebibliotek for informasjonsgjenfinning, opprinnelig utviklet i programmeringsspråket Java av Doug Cutting. Lucene støttes av Apache Software Foundation og utgis under Apache-lisensen.");
    assertLang("en", "id", "2en", "name", "Lucene", "subject", "Apache Lucene is a free/open source information retrieval software library, originally created in Java by Doug Cutting. It is supported by the Apache Software Foundation and is released under the Apache Software License.");
    assertLang("sv", "id", "3sv", "name", "Maven", "subject", "Apache Maven är ett verktyg utvecklat av Apache Software Foundation och används inom systemutveckling av datorprogram i programspråket Java. Maven används för att automatiskt paketera (bygga) programfilerna till en distribuerbar enhet. Maven används inom samma område som Apache Ant men dess byggfiler är deklarativa till skillnad ifrån Ants skriptbaserade.");
    assertLang("es", "id", "4es", "name", "Lucene", "subject", "Lucene es un API de código abierto para recuperación de información, originalmente implementada en Java por Doug Cutting. Está apoyado por el Apache Software Foundation y se distribuye bajo la Apache Software License. Lucene tiene versiones para otros lenguajes incluyendo Delphi, Perl, C#, C++, Python, Ruby y PHP.");
    assertLang("un", "id", "5un", "name", "a", "subject", "b");
    assertLang("th", "id", "6th", "name", "บทความคัดสรรเดือนนี้", "subject", "อันเนอลีส มารี อันเนอ ฟรังค์ หรือมักรู้จักในภาษาไทยว่า แอนน์ แฟรงค์ เป็นเด็กหญิงชาวยิว เกิดที่เมืองแฟรงก์เฟิร์ต ประเทศเยอรมนี เธอมีชื่อเสียงโด่งดังในฐานะผู้เขียนบันทึกประจำวันซึ่งต่อมาได้รับการตีพิมพ์เป็นหนังสือ บรรยายเหตุการณ์ขณะหลบซ่อนตัวจากการล่าชาวยิวในประเทศเนเธอร์แลนด์ ระหว่างที่ถูกเยอรมนีเข้าครอบครองในช่วงสงครามโลกครั้งที่สอง");
    assertLang("ru", "id", "7ru", "name", "Lucene", "subject", "The Apache Lucene — это свободная библиотека для высокоскоростного полнотекстового поиска, написанная на Java. Может быть использована для поиска в интернете и других областях компьютерной лингвистики (аналитическая философия).");
    assertLang("de", "id", "8de", "name", "Lucene", "subject", "Lucene ist ein Freie-Software-Projekt der Apache Software Foundation, das eine Suchsoftware erstellt. Durch die hohe Leistungsfähigkeit und Skalierbarkeit können die Lucene-Werkzeuge für beliebige Projektgrößen und Anforderungen eingesetzt werden. So setzt beispielsweise Wikipedia Lucene für die Volltextsuche ein. Zudem verwenden die beiden Desktop-Suchprogramme Beagle und Strigi eine C#- bzw. C++- Portierung von Lucene als Indexer.");
    assertLang("fr", "id", "9fr", "name", "Lucene", "subject", "Lucene est un moteur de recherche libre écrit en Java qui permet d'indexer et de rechercher du texte. C'est un projet open source de la fondation Apache mis à disposition sous licence Apache. Il est également disponible pour les langages Ruby, Perl, C++, PHP.");
    assertLang("nl", "id", "10nl", "name", "Lucene", "subject", "Lucene is een gratis open source, tekst gebaseerde information retrieval API van origine geschreven in Java door Doug Cutting. Het wordt ondersteund door de Apache Software Foundation en is vrijgegeven onder de Apache Software Licentie. Lucene is ook beschikbaar in andere programeertalen zoals Perl, C#, C++, Python, Ruby en PHP.");
    assertLang("it", "id", "11it", "name", "Lucene", "subject", "Lucene è una API gratuita ed open source per il reperimento di informazioni inizialmente implementata in Java da Doug Cutting. È supportata dall'Apache Software Foundation ed è resa disponibile con l'Apache License. Lucene è stata successivamente reimplementata in Perl, C#, C++, Python, Ruby e PHP.");
    assertLang("pt", "id", "12pt", "name", "Lucene", "subject", "Apache Lucene, ou simplesmente Lucene, é um software de busca e uma API de indexação de documentos, escrito na linguagem de programação Java. É um software de código aberto da Apache Software Foundation licenciado através da licença Apache.");
  }
  
  @Test
  public void testMapFieldName() throws Exception {
    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "name");
    parameters.add("langid.map.lcmap", "jp:s zh:cjk ko:cjk");
    parameters.add("langid.enforceSchema", "true");
    liProcessor = createLangIdProcessor(parameters);
    
    assertEquals("test_no", liProcessor.getMappedField("test", "no"));
    assertEquals("test_en", liProcessor.getMappedField("test", "en"));
    assertEquals("test_s", liProcessor.getMappedField("test", "jp"));
    assertEquals("test_cjk", liProcessor.getMappedField("test", "zh"));
    assertEquals("test_cjk", liProcessor.getMappedField("test", "ko"));

    // Prove support for other mapping regex
    parameters.add("langid.map.pattern", "text_(.*?)_field");
    parameters.add("langid.map.replace", "$1_{lang}Text");
    liProcessor = createLangIdProcessor(parameters);

    assertEquals("title_noText", liProcessor.getMappedField("text_title_field", "no"));
    assertEquals("body_svText", liProcessor.getMappedField("text_body_field", "sv"));
  }

  @Test
  public void testPreExisting() throws Exception {
    SolrInputDocument doc;
    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "text");
    parameters.add("langid.langField", "language");
    parameters.add("langid.langsField", "languages");
    parameters.add("langid.enforceSchema", "false");
    parameters.add("langid.map", "true");
    liProcessor = createLangIdProcessor(parameters);
    
    doc = englishDoc();
    assertEquals("en", liProcessor.process(doc).getFieldValue("language"));
    assertEquals("en", liProcessor.process(doc).getFieldValue("languages"));
    
    doc = englishDoc();
    doc.setField("language", "no");
    assertEquals("no", liProcessor.process(doc).getFieldValue("language"));
    assertEquals("no", liProcessor.process(doc).getFieldValue("languages"));
    assertNotNull(liProcessor.process(doc).getFieldValue("text_no"));
  }

  @Test
  public void testDefaultFallbackEmptyString() throws Exception {
    SolrInputDocument doc;
    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "text");
    parameters.add("langid.langField", "language");
    parameters.add("langid.enforceSchema", "false");
    liProcessor = createLangIdProcessor(parameters);
    
    doc = tooShortDoc();
    assertEquals("", liProcessor.process(doc).getFieldValue("language"));
  }

  @Test
  public void testFallback() throws Exception {
    SolrInputDocument doc;
    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "text");
    parameters.add("langid.langField", "language");
    parameters.add("langid.fallbackFields", "noop,fb");
    parameters.add("langid.fallback", "fbVal");
    parameters.add("langid.enforceSchema", "false");
    liProcessor = createLangIdProcessor(parameters);
      
    // Verify fallback to field fb (noop field does not exist and is skipped)
    doc = tooShortDoc();
    doc.addField("fb", "fbField");
    assertEquals("fbField", liProcessor.process(doc).getFieldValue("language"));

    // Verify fallback to fallback value since no fallback fields exist
    doc = tooShortDoc();
    assertEquals("fbVal", liProcessor.process(doc).getFieldValue("language"));  
  }
  
  @Test
  public void testResolveLanguage() throws Exception {
    List<DetectedLanguage> langs;
    parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "text");
    parameters.add("langid.langField", "language");
    liProcessor = createLangIdProcessor(parameters);

    // No detected languages
    langs = new ArrayList<DetectedLanguage>();
    assertEquals("", liProcessor.resolveLanguage(langs, null));
    assertEquals("fallback", liProcessor.resolveLanguage(langs, "fallback"));

    // One detected language
    langs.add(new DetectedLanguage("one", 1.0));
    assertEquals("one", liProcessor.resolveLanguage(langs, "fallback"));    

    // One detected language under default threshold
    langs = new ArrayList<DetectedLanguage>();
    langs.add(new DetectedLanguage("under", 0.1));
    assertEquals("fallback", liProcessor.resolveLanguage(langs, "fallback"));    
  }
  
  
  // Various utility methods
  
  private SolrInputDocument englishDoc() {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("text", "Apache Lucene is a free/open source information retrieval software library, originally created in Java by Doug Cutting. It is supported by the Apache Software Foundation and is released under the Apache Software License.");
    return doc;
  }

  private SolrInputDocument tooShortDoc() {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("text", "This text is too short");
    return doc;
  }

  private LanguageIdentifierUpdateProcessor createLangIdProcessor(ModifiableSolrParams parameters) throws Exception {
    return new LanguageIdentifierUpdateProcessor(_parser.buildRequestFrom(null, parameters, null), resp, null);
  }

  private void assertLang(String langCode, String... fieldsAndValues) throws Exception {
    if(liProcessor == null)
      throw new Exception("Processor must be initialized before calling assertLang()");
    SolrInputDocument doc = sid(fieldsAndValues);
    assertEquals(langCode, liProcessor.process(doc).getFieldValue(liProcessor.langField));
  }
  
  private SolrInputDocument sid(String... fieldsAndValues) {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fieldsAndValues.length; i+=2) {
      doc.addField(fieldsAndValues[i], fieldsAndValues[i+1]);
    }
    return doc;
  }
}

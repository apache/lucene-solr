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

public class LangDetectLanguageIdentifierUpdateProcessorFactoryTest extends LanguageIdentifierUpdateProcessorFactoryTestCase {
  @Override
  protected LanguageIdentifierUpdateProcessor createLangIdProcessor(ModifiableSolrParams parameters) throws Exception {
    return new LangDetectLanguageIdentifierUpdateProcessor(_parser.buildRequestFrom(h.getCore(), parameters, null), resp, null);
  }
  
  // this one actually works better it seems with short docs
  @Override
  protected SolrInputDocument tooShortDoc() {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("text", "");
    return doc;
  }
  
  /* we don't return 'un' for the super-short one (this detector things hungarian?).
   * replace this with japanese
   */
  @Test @Override
  public void testLangIdGlobal() throws Exception {
    ModifiableSolrParams parameters = new ModifiableSolrParams();
    parameters.add("langid.fl", "name,subject");
    parameters.add("langid.langField", "language_s");
    parameters.add("langid.fallback", "un");
    liProcessor = createLangIdProcessor(parameters);
    
    assertLang("no", "id", "1no", "name", "Lucene", "subject", "Lucene er et fri/åpen kildekode programvarebibliotek for informasjonsgjenfinning, opprinnelig utviklet i programmeringsspråket Java av Doug Cutting. Lucene støttes av Apache Software Foundation og utgis under Apache-lisensen.");
    assertLang("en", "id", "2en", "name", "Lucene", "subject", "Apache Lucene is a free/open source information retrieval software library, originally created in Java by Doug Cutting. It is supported by the Apache Software Foundation and is released under the Apache Software License.");
    assertLang("sv", "id", "3sv", "name", "Maven", "subject", "Apache Maven är ett verktyg utvecklat av Apache Software Foundation och används inom systemutveckling av datorprogram i programspråket Java. Maven används för att automatiskt paketera (bygga) programfilerna till en distribuerbar enhet. Maven används inom samma område som Apache Ant men dess byggfiler är deklarativa till skillnad ifrån Ants skriptbaserade.");
    assertLang("es", "id", "4es", "name", "Lucene", "subject", "Lucene es un API de código abierto para recuperación de información, originalmente implementada en Java por Doug Cutting. Está apoyado por el Apache Software Foundation y se distribuye bajo la Apache Software License. Lucene tiene versiones para otros lenguajes incluyendo Delphi, Perl, C#, C++, Python, Ruby y PHP.");
    assertLang("ja", "id", "5ja", "name", "Japanese", "subject", "日本語（にほんご、にっぽんご）は主として、日本で使用されてきた言語である。日本国は法令上、公用語を明記していないが、事実上の公用語となっており、学校教育の「国語」で教えられる。");
    assertLang("th", "id", "6th", "name", "บทความคัดสรรเดือนนี้", "subject", "อันเนอลีส มารี อันเนอ ฟรังค์ หรือมักรู้จักในภาษาไทยว่า แอนน์ แฟรงค์ เป็นเด็กหญิงชาวยิว เกิดที่เมืองแฟรงก์เฟิร์ต ประเทศเยอรมนี เธอมีชื่อเสียงโด่งดังในฐานะผู้เขียนบันทึกประจำวันซึ่งต่อมาได้รับการตีพิมพ์เป็นหนังสือ บรรยายเหตุการณ์ขณะหลบซ่อนตัวจากการล่าชาวยิวในประเทศเนเธอร์แลนด์ ระหว่างที่ถูกเยอรมนีเข้าครอบครองในช่วงสงครามโลกครั้งที่สอง");
    assertLang("ru", "id", "7ru", "name", "Lucene", "subject", "The Apache Lucene — это свободная библиотека для высокоскоростного полнотекстового поиска, написанная на Java. Может быть использована для поиска в интернете и других областях компьютерной лингвистики (аналитическая философия).");
    assertLang("de", "id", "8de", "name", "Lucene", "subject", "Lucene ist ein Freie-Software-Projekt der Apache Software Foundation, das eine Suchsoftware erstellt. Durch die hohe Leistungsfähigkeit und Skalierbarkeit können die Lucene-Werkzeuge für beliebige Projektgrößen und Anforderungen eingesetzt werden. So setzt beispielsweise Wikipedia Lucene für die Volltextsuche ein. Zudem verwenden die beiden Desktop-Suchprogramme Beagle und Strigi eine C#- bzw. C++- Portierung von Lucene als Indexer.");
    assertLang("fr", "id", "9fr", "name", "Lucene", "subject", "Lucene est un moteur de recherche libre écrit en Java qui permet d'indexer et de rechercher du texte. C'est un projet open source de la fondation Apache mis à disposition sous licence Apache. Il est également disponible pour les langages Ruby, Perl, C++, PHP.");
    assertLang("nl", "id", "10nl", "name", "Lucene", "subject", "Lucene is een gratis open source, tekst gebaseerde information retrieval API van origine geschreven in Java door Doug Cutting. Het wordt ondersteund door de Apache Software Foundation en is vrijgegeven onder de Apache Software Licentie. Lucene is ook beschikbaar in andere programeertalen zoals Perl, C#, C++, Python, Ruby en PHP.");
    assertLang("it", "id", "11it", "name", "Lucene", "subject", "Lucene è una API gratuita ed open source per il reperimento di informazioni inizialmente implementata in Java da Doug Cutting. È supportata dall'Apache Software Foundation ed è resa disponibile con l'Apache License. Lucene è stata successivamente reimplementata in Perl, C#, C++, Python, Ruby e PHP.");
    assertLang("pt", "id", "12pt", "name", "Lucene", "subject", "Apache Lucene, ou simplesmente Lucene, é um software de busca e uma API de indexação de documentos, escrito na linguagem de programação Java. É um software de código aberto da Apache Software Foundation licenciado através da licença Apache.");
  }
}

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

public abstract class LanguageIdentifierUpdateProcessorFactoryTestCase extends SolrTestCaseJ4 {

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
    assertLang("es", "id", "4es", "name", "Español", "subject", "El español, como las otras lenguas romances, es una continuación moderna del latín hablado (denominado latín vulgar), desde el siglo III, que tras el desmembramiento del Imperio romano fue divergiendo de las otras variantes del latín que se hablaban en las distintas provincias del antiguo Imperio, dando lugar mediante una lenta evolución a las distintas lenguas romances. Debido a su propagación por América, el español es, con diferencia, la lengua romance que ha logrado mayor difusión.");
    assertLang("un", "id", "5un", "name", "a", "subject", "b");
    assertLang("th", "id", "6th", "name", "บทความคัดสรรเดือนนี้", "subject", "อันเนอลีส มารี อันเนอ ฟรังค์ หรือมักรู้จักในภาษาไทยว่า แอนน์ แฟรงค์ เป็นเด็กหญิงชาวยิว เกิดที่เมืองแฟรงก์เฟิร์ต ประเทศเยอรมนี เธอมีชื่อเสียงโด่งดังในฐานะผู้เขียนบันทึกประจำวันซึ่งต่อมาได้รับการตีพิมพ์เป็นหนังสือ บรรยายเหตุการณ์ขณะหลบซ่อนตัวจากการล่าชาวยิวในประเทศเนเธอร์แลนด์ ระหว่างที่ถูกเยอรมนีเข้าครอบครองในช่วงสงครามโลกครั้งที่สอง");
    assertLang("ru", "id", "7ru", "name", "Lucene", "subject", "The Apache Lucene — это свободная библиотека для высокоскоростного полнотекстового поиска, написанная на Java. Может быть использована для поиска в интернете и других областях компьютерной лингвистики (аналитическая философия).");
    assertLang("de", "id", "8de", "name", "Lucene", "subject", "Lucene ist ein Freie-Software-Projekt der Apache Software Foundation, das eine Suchsoftware erstellt. Durch die hohe Leistungsfähigkeit und Skalierbarkeit können die Lucene-Werkzeuge für beliebige Projektgrößen und Anforderungen eingesetzt werden. So setzt beispielsweise Wikipedia Lucene für die Volltextsuche ein. Zudem verwenden die beiden Desktop-Suchprogramme Beagle und Strigi eine C#- bzw. C++- Portierung von Lucene als Indexer.");
    assertLang("fr", "id", "9fr", "name", "Lucene", "subject", "Lucene est un moteur de recherche libre écrit en Java qui permet d'indexer et de rechercher du texte. C'est un projet open source de la fondation Apache mis à disposition sous licence Apache. Il est également disponible pour les langages Ruby, Perl, C++, PHP.");
    assertLang("nl", "id", "10nl", "name", "Lucene", "subject", "Lucene is een gratis open source, tekst gebaseerde information retrieval API van origine geschreven in Java door Doug Cutting. Het wordt ondersteund door de Apache Software Foundation en is vrijgegeven onder de Apache Software Licentie. Lucene is ook beschikbaar in andere programeertalen zoals Perl, C#, C++, Python, Ruby en PHP.");
    assertLang("it", "id", "11it", "name", "Lucene", "subject", "Lucene è una API gratuita ed open source per il reperimento di informazioni inizialmente implementata in Java da Doug Cutting. È supportata dall'Apache Software Foundation ed è resa disponibile con l'Apache License. Lucene è stata successivamente reimplementata in Perl, C#, C++, Python, Ruby e PHP.");
    assertLang("pt", "id", "12pt", "name", "Lucene", "subject", "Apache Lucene, ou simplesmente Lucene, é um software de busca e uma API de indexação de documentos, escrito na linguagem de programação Java. É um software de código aberto da Apache Software Foundation licenciado através da licença Apache.");
    // New in Tika1.0
    assertLang("ca", "id", "13ca", "name", "Catalan", "subject", "El català posseeix dos estàndards principals: el regulat per l'Institut d'Estudis Catalans, o estàndard general, que pren com a base l'ortografia establerta per Pompeu Fabra amb els trets gramaticals i ortogràfics característics del català central; i el regulat per l'Acadèmia Valenciana de la Llengua, estàndard d'àmbit restringit, centrat en l'estandardització del valencià i que pren com a base les Normes de Castelló, és a dir, l'ortografia de Pompeu Fabra però més adaptada a la pronúncia del català occidental i als trets que caracteritzen els dialectes valencians.");
    assertLang("be", "id", "14be", "name", "Belarusian", "subject", "Наступнай буйной дзяржавай на беларускай зямлі было Вялікае княства Літоўскае, Рускае і Жамойцкае (ВКЛ). Падчас стварэння і пачатковага развіцця гэтай дзяржавы найбуйнейшым і асноўным яе цэнтрам быў Новагародак. Акрамя сучасных земляў Беларусі, у склад гэтай дзяржавы ўваходзілі таксама землі сучаснай Літвы, паўночная частка сучаснай Украіны і частка сучаснай Расіі.");
    assertLang("eo", "id", "15eo", "name", "Esperanto", "subject", "La vortprovizo de Esperanto devenas plejparte el la okcidenteŭropaj lingvoj, dum ĝia sintakso kaj morfologio montras ankaŭ slavlingvan influon. La morfemoj ne ŝanĝiĝas kaj oni povas ilin preskaŭ senlime kombini, kreante diverssignifajn vortojn, Esperanto do havas multajn kunaĵojn kun la analizaj lingvoj, al kiuj apartenas ekzemple la ĉina; kontraŭe la interna strukturo de Esperanto certagrade respegulas la aglutinajn lingvojn, kiel la japanan, svahilan aŭ turkan.");
    assertLang("gl", "id", "16gl", "name", "Galician", "subject", "A cifra de falantes medrou axiña durante as décadas seguintes, nun principio no Imperio ruso e na Europa oriental, logo na Europa occidental, América, China e no Xapón. Nos primeiros anos do movemento, os esperantistas mantiñan contacto por correspondencia, pero en 1905 o primeiro Congreso Universal de Esperanto levouse a cabo na cidade francesa de Boulogne-sur-Mer. Dende entón, os congresos mundiais organizáronse nos cinco continentes ano tras ano agás durante as dúas Guerras Mundiais.");
    assertLang("ro", "id", "17ro", "name", "Romanian", "subject", "La momentul destrămării Uniunii Sovietice și a înlăturării regimului comunist instalat în România (1989), țara a inițiat o serie de reforme economice și politice. După un deceniu de probleme economice, România a introdus noi reforme economice de ordin general (precum cota unică de impozitare, în 2005) și a aderat la Uniunea Europeană la 1 ianuarie 2007.");
    assertLang("sk", "id", "18sk", "name", "Slovakian", "subject", "Boli vytvorené dva národné parlamenty - Česká národná rada a Slovenská národná rada a spoločný jednokomorový česko-slovenský parlament bol premenovaný z Národného zhromaždenia na Federálne zhromaždenie s dvoma komorami - Snemovňou ľudu a Snemovňu národov.");
    assertLang("sl", "id", "19sl", "name", "Slovenian", "subject", "Slovenska Wikipedija je različica spletne enciklopedije Wikipedije v slovenskem jeziku. Projekt slovenske Wikipedije se je začel 26. februarja 2002 z ustanovitvijo njene spletne strani, njen pobudnik pa je bil uporabnik Jani Melik.");
    assertLang("uk", "id", "20uk", "name", "Ukrainian", "subject", "Народно-господарський комплекс країни включає такі види промисловості як важке машинобудування, чорна та кольорова металургія, суднобудування, виробництво автобусів, легкових та вантажних автомобілів, тракторів та іншої сільськогосподарської техніки, тепловозів, верстатів, турбін, авіаційних двигунів та літаків, обладнання для електростанцій, нафто-газової та хімічної промисловості тощо. Крім того, Україна є потужним виробником електроенергії. Україна має розвинуте сільське господарство і займає одне з провідних місць серед експортерів деяких видів сільськогосподарської продукції і продовольства (зокрема, соняшникової олії).");
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

  protected SolrInputDocument tooShortDoc() {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("text", "This text is too short");
    return doc;
  }

  protected abstract LanguageIdentifierUpdateProcessor createLangIdProcessor(ModifiableSolrParams parameters) throws Exception;

  protected void assertLang(String langCode, String... fieldsAndValues) throws Exception {
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

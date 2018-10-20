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
package org.apache.lucene.analysis.nl;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArrayMap;
import org.apache.lucene.analysis.CharArraySet;

/**
 * Test the Dutch Stem Filter, which only modifies the term text.
 * 
 * The code states that it uses the snowball algorithm, but tests reveal some differences.
 * 
 */
public class TestDutchAnalyzer extends BaseTokenStreamTestCase {
  
  public void testWithSnowballExamples() throws Exception {
   check("lichaamsziek", "lichaamsziek");
   check("lichamelijk", "licham");
   check("lichamelijke", "licham");
   check("lichamelijkheden", "licham");
   check("lichamen", "licham");
   check("lichere", "licher");
   check("licht", "licht");
   check("lichtbeeld", "lichtbeeld");
   check("lichtbruin", "lichtbruin");
   check("lichtdoorlatende", "lichtdoorlat");
   check("lichte", "licht");
   check("lichten", "licht");
   check("lichtende", "lichtend");
   check("lichtenvoorde", "lichtenvoord");
   check("lichter", "lichter");
   check("lichtere", "lichter");
   check("lichters", "lichter");
   check("lichtgevoeligheid", "lichtgevoel");
   check("lichtgewicht", "lichtgewicht");
   check("lichtgrijs", "lichtgrijs");
   check("lichthoeveelheid", "lichthoevel");
   check("lichtintensiteit", "lichtintensiteit");
   check("lichtje", "lichtj");
   check("lichtjes", "lichtjes");
   check("lichtkranten", "lichtkrant");
   check("lichtkring", "lichtkring");
   check("lichtkringen", "lichtkring");
   check("lichtregelsystemen", "lichtregelsystem");
   check("lichtste", "lichtst");
   check("lichtstromende", "lichtstrom");
   check("lichtte", "licht");
   check("lichtten", "licht");
   check("lichttoetreding", "lichttoetred");
   check("lichtverontreinigde", "lichtverontreinigd");
   check("lichtzinnige", "lichtzinn");
   check("lid", "lid");
   check("lidia", "lidia");
   check("lidmaatschap", "lidmaatschap");
   check("lidstaten", "lidstat");
   check("lidvereniging", "lidveren");
   check("opgingen", "opging");
   check("opglanzing", "opglanz");
   check("opglanzingen", "opglanz");
   check("opglimlachten", "opglimlacht");
   check("opglimpen", "opglimp");
   check("opglimpende", "opglimp");
   check("opglimping", "opglimp");
   check("opglimpingen", "opglimp");
   check("opgraven", "opgrav");
   check("opgrijnzen", "opgrijnz");
   check("opgrijzende", "opgrijz");
   check("opgroeien", "opgroei");
   check("opgroeiende", "opgroei");
   check("opgroeiplaats", "opgroeiplat");
   check("ophaal", "ophal");
   check("ophaaldienst", "ophaaldienst");
   check("ophaalkosten", "ophaalkost");
   check("ophaalsystemen", "ophaalsystem");
   check("ophaalt", "ophaalt");
   check("ophaaltruck", "ophaaltruck");
   check("ophalen", "ophal");
   check("ophalend", "ophal");
   check("ophalers", "ophaler");
   check("ophef", "ophef");
   check("opheldering", "ophelder");
   check("ophemelde", "ophemeld");
   check("ophemelen", "ophemel");
   check("opheusden", "opheusd");
   check("ophief", "ophief");
   check("ophield", "ophield");
   check("ophieven", "ophiev");
   check("ophoepelt", "ophoepelt");
   check("ophoog", "ophog");
   check("ophoogzand", "ophoogzand");
   check("ophopen", "ophop");
   check("ophoping", "ophop");
   check("ophouden", "ophoud");
  }
  
  public void testSnowballCorrectness() throws Exception {
    Analyzer a = new DutchAnalyzer();
    checkOneTerm(a, "opheffen", "opheff");
    checkOneTerm(a, "opheffende", "opheff");
    checkOneTerm(a, "opheffing", "opheff");
    a.close();
  }
  
  public void testReusableTokenStream() throws Exception {
    Analyzer a = new DutchAnalyzer(); 
    checkOneTerm(a, "lichaamsziek", "lichaamsziek");
    checkOneTerm(a, "lichamelijk", "licham");
    checkOneTerm(a, "lichamelijke", "licham");
    checkOneTerm(a, "lichamelijkheden", "licham");
    a.close();
  }
  
  public void testExclusionTableViaCtor() throws IOException {
    CharArraySet set = new CharArraySet( 1, true);
    set.add("lichamelijk");
    DutchAnalyzer a = new DutchAnalyzer( CharArraySet.EMPTY_SET, set);
    assertAnalyzesTo(a, "lichamelijk lichamelijke", new String[] { "lichamelijk", "licham" });
    a.close();

    a = new DutchAnalyzer( CharArraySet.EMPTY_SET, set);
    assertAnalyzesTo(a, "lichamelijk lichamelijke", new String[] { "lichamelijk", "licham" });
    a.close();
  }
  
  /** 
   * check that the default stem overrides are used
   * even if you use a non-default ctor.
   */
  public void testStemOverrides() throws IOException {
    DutchAnalyzer a = new DutchAnalyzer( CharArraySet.EMPTY_SET);
    checkOneTerm(a, "fiets", "fiets");
    a.close();
  }
  
  public void testEmptyStemDictionary() throws IOException {
    DutchAnalyzer a = new DutchAnalyzer( CharArraySet.EMPTY_SET, 
        CharArraySet.EMPTY_SET, CharArrayMap.<String>emptyMap());
    checkOneTerm(a, "fiets", "fiet");
    a.close();
  }
  
  /**
   * Test that stopwords are not case sensitive
   */
  public void testStopwordsCasing() throws IOException {
    DutchAnalyzer a = new DutchAnalyzer();
    assertAnalyzesTo(a, "Zelf", new String[] { });
    a.close();
  }
  
  private void check(final String input, final String expected) throws Exception {
    Analyzer analyzer = new DutchAnalyzer();
    checkOneTerm(analyzer, input, expected);
    analyzer.close();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer analyzer = new DutchAnalyzer();
    checkRandomData(random(), analyzer, 1000*RANDOM_MULTIPLIER);
    analyzer.close();
  }
  
}

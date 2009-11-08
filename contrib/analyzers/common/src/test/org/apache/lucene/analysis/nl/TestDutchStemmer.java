package org.apache.lucene.analysis.nl;

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

import java.io.File;
import java.io.Reader;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.util.Version;

/**
 * Test the Dutch Stem Filter, which only modifies the term text.
 * 
 * The code states that it uses the snowball algorithm, but tests reveal some differences.
 * 
 */
public class TestDutchStemmer extends BaseTokenStreamTestCase {
  File dataDir = new File(System.getProperty("dataDir", "./bin"));
  File customDictFile = new File(dataDir, "org/apache/lucene/analysis/nl/customStemDict.txt");
  
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
	 check("opheffen", "ophef"); // versus snowball 'opheff'
	 check("opheffende", "ophef"); // versus snowball 'opheff'
	 check("opheffing", "ophef"); // versus snowball 'opheff'
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
  
  public void testReusableTokenStream() throws Exception {
    Analyzer a = new DutchAnalyzer(Version.LUCENE_CURRENT); 
    checkOneTermReuse(a, "lichaamsziek", "lichaamsziek");
    checkOneTermReuse(a, "lichamelijk", "licham");
    checkOneTermReuse(a, "lichamelijke", "licham");
    checkOneTermReuse(a, "lichamelijkheden", "licham");
  }
  
  /**
   * subclass that acts just like whitespace analyzer for testing
   */
  private class DutchSubclassAnalyzer extends DutchAnalyzer {
    public DutchSubclassAnalyzer(Version matchVersion) {
      super(matchVersion);
    }
    @Override
    public TokenStream tokenStream(String fieldName, Reader reader) {
      return new WhitespaceTokenizer(reader);
    }
  }
  
  public void testLUCENE1678BWComp() throws Exception {
    Analyzer a = new DutchSubclassAnalyzer(Version.LUCENE_CURRENT);
    checkOneTermReuse(a, "lichaamsziek", "lichaamsziek");
    checkOneTermReuse(a, "lichamelijk", "lichamelijk");
    checkOneTermReuse(a, "lichamelijke", "lichamelijke");
    checkOneTermReuse(a, "lichamelijkheden", "lichamelijkheden");
  }
 
  /* 
   * Test that changes to the exclusion table are applied immediately
   * when using reusable token streams.
   */
  public void testExclusionTableReuse() throws Exception {
    DutchAnalyzer a = new DutchAnalyzer(Version.LUCENE_CURRENT);
    checkOneTermReuse(a, "lichamelijk", "licham");
    a.setStemExclusionTable(new String[] { "lichamelijk" });
    checkOneTermReuse(a, "lichamelijk", "lichamelijk");
  }
  
  /* 
   * Test that changes to the dictionary stemming table are applied immediately
   * when using reusable token streams.
   */
  public void testStemDictionaryReuse() throws Exception {
    DutchAnalyzer a = new DutchAnalyzer(Version.LUCENE_CURRENT);
    checkOneTermReuse(a, "lichamelijk", "licham");
    a.setStemDictionary(customDictFile);
    checkOneTermReuse(a, "lichamelijk", "somethingentirelydifferent");
  }
  
  private void check(final String input, final String expected) throws Exception {
    checkOneTerm(new DutchAnalyzer(Version.LUCENE_CURRENT), input, expected); 
  }
  
}
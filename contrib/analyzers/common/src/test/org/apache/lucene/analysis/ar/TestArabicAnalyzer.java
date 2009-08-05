package org.apache.lucene.analysis.ar;

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

import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import junit.framework.TestCase;

/**
 * Test the Arabic Analyzer
 *
 */
public class TestArabicAnalyzer extends TestCase {
  
  /** This test fails with NPE when the 
   * stopwords file is missing in classpath */
  public void testResourcesAvailable() {
    new ArabicAnalyzer();
  }
  
  /**
   * Some simple tests showing some features of the analyzer, how some regular forms will conflate
   */
  public void testBasicFeatures() throws Exception {
    ArabicAnalyzer a = new ArabicAnalyzer();
    assertAnalyzesTo(a, "كبير", new String[] { "كبير" });
    assertAnalyzesTo(a, "كبيرة", new String[] { "كبير" }); // feminine marker
    
    assertAnalyzesTo(a, "مشروب", new String[] { "مشروب" });
    assertAnalyzesTo(a, "مشروبات", new String[] { "مشروب" }); // plural -at
    
    assertAnalyzesTo(a, "أمريكيين", new String[] { "امريك" }); // plural -in
    assertAnalyzesTo(a, "امريكي", new String[] { "امريك" }); // singular with bare alif
    
    assertAnalyzesTo(a, "كتاب", new String[] { "كتاب" }); 
    assertAnalyzesTo(a, "الكتاب", new String[] { "كتاب" }); // definite article
    
    assertAnalyzesTo(a, "ما ملكت أيمانكم", new String[] { "ملكت", "ايمانكم"});
    assertAnalyzesTo(a, "الذين ملكت أيمانكم", new String[] { "ملكت", "ايمانكم" }); // stopwords
  }

  /**
   * Non-arabic text gets treated in a similar way as SimpleAnalyzer.
   */
  public void testEnglishInput() throws Exception {
    assertAnalyzesTo(new ArabicAnalyzer(), "English text.", new String[] {
        "english", "text" });
  }
  
  private void assertAnalyzesTo(Analyzer a, String input, String[] output)
      throws Exception {
    TokenStream ts = a.tokenStream("dummy", new StringReader(input));
    TermAttribute termAtt = (TermAttribute) ts
        .getAttribute(TermAttribute.class);

    for (int i = 0; i < output.length; i++) {
      assertTrue(ts.incrementToken());
      assertEquals(output[i], termAtt.term());
    }

    assertFalse(ts.incrementToken());
    ts.close();
  }

}

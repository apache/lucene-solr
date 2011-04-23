package org.apache.lucene.analysis.th;

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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.util.Version;

/**
 * Test case for ThaiAnalyzer, modified from TestFrenchAnalyzer
 *
 * @version   0.1
 */

public class TestThaiAnalyzer extends BaseTokenStreamTestCase {
	
	/* 
	 * testcase for offsets
	 */
	public void testOffsets() throws Exception {
	  assumeTrue("JRE does not support Thai dictionary-based BreakIterator", ThaiWordFilter.DBBI_AVAILABLE);
		assertAnalyzesTo(new ThaiAnalyzer(TEST_VERSION_CURRENT), "การที่ได้ต้องแสดงว่างานดี", 
		    new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี" },
				new int[] { 0, 3, 6, 9, 13, 17, 20, 23 },
				new int[] { 3, 6, 9, 13, 17, 20, 23, 25 });
	}
	
	public void testTokenType() throws Exception {
	    assumeTrue("JRE does not support Thai dictionary-based BreakIterator", ThaiWordFilter.DBBI_AVAILABLE);
      assertAnalyzesTo(new ThaiAnalyzer(TEST_VERSION_CURRENT), "การที่ได้ต้องแสดงว่างานดี ๑๒๓", 
                       new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี", "๑๒๓" },
                       new String[] { "<SOUTHEAST_ASIAN>", "<SOUTHEAST_ASIAN>", 
                                      "<SOUTHEAST_ASIAN>", "<SOUTHEAST_ASIAN>", 
                                      "<SOUTHEAST_ASIAN>", "<SOUTHEAST_ASIAN>",
                                      "<SOUTHEAST_ASIAN>", "<SOUTHEAST_ASIAN>",
                                      "<NUM>" });
	}

	/**
	 * Thai numeric tokens were typed as <ALPHANUM> instead of <NUM>.
	 * @deprecated testing backwards behavior
 	 */
	@Deprecated
	public void testBuggyTokenType30() throws Exception {
	  assumeTrue("JRE does not support Thai dictionary-based BreakIterator", ThaiWordFilter.DBBI_AVAILABLE);
		assertAnalyzesTo(new ThaiAnalyzer(Version.LUCENE_30), "การที่ได้ต้องแสดงว่างานดี ๑๒๓", 
                         new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี", "๑๒๓" },
                         new String[] { "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", 
                                        "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", 
                                        "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>" });
	}
	
	/** @deprecated testing backwards behavior */
	@Deprecated
    public void testAnalyzer30() throws Exception {
	  assumeTrue("JRE does not support Thai dictionary-based BreakIterator", ThaiWordFilter.DBBI_AVAILABLE);
        ThaiAnalyzer analyzer = new ThaiAnalyzer(Version.LUCENE_30);
	
		assertAnalyzesTo(analyzer, "", new String[] {});

		assertAnalyzesTo(
			analyzer,
			"การที่ได้ต้องแสดงว่างานดี",
			new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี"});

		assertAnalyzesTo(
			analyzer,
			"บริษัทชื่อ XY&Z - คุยกับ xyz@demo.com",
			new String[] { "บริษัท", "ชื่อ", "xy&z", "คุย", "กับ", "xyz@demo.com" });

    // English stop words
		assertAnalyzesTo(
			analyzer,
			"ประโยคว่า The quick brown fox jumped over the lazy dogs",
			new String[] { "ประโยค", "ว่า", "quick", "brown", "fox", "jumped", "over", "lazy", "dogs" });
	}
	
	/*
	 * Test that position increments are adjusted correctly for stopwords.
	 */
	public void testPositionIncrements() throws Exception {
	  assumeTrue("JRE does not support Thai dictionary-based BreakIterator", ThaiWordFilter.DBBI_AVAILABLE);
	  ThaiAnalyzer analyzer = new ThaiAnalyzer(TEST_VERSION_CURRENT);

    assertAnalyzesTo(new ThaiAnalyzer(TEST_VERSION_CURRENT), "การที่ได้ต้อง the แสดงว่างานดี", 
        new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี" },
        new int[] { 0, 3, 6, 9, 18, 22, 25, 28 },
        new int[] { 3, 6, 9, 13, 22, 25, 28, 30 },
        new int[] { 1, 1, 1, 1, 2, 1, 1, 1 });
	 
	  // case that a stopword is adjacent to thai text, with no whitespace
    assertAnalyzesTo(new ThaiAnalyzer(TEST_VERSION_CURRENT), "การที่ได้ต้องthe แสดงว่างานดี", 
        new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี" },
        new int[] { 0, 3, 6, 9, 17, 21, 24, 27 },
        new int[] { 3, 6, 9, 13, 21, 24, 27, 29 },
        new int[] { 1, 1, 1, 1, 2, 1, 1, 1 });
	}
	
	public void testReusableTokenStream() throws Exception {
	  assumeTrue("JRE does not support Thai dictionary-based BreakIterator", ThaiWordFilter.DBBI_AVAILABLE);
	  ThaiAnalyzer analyzer = new ThaiAnalyzer(TEST_VERSION_CURRENT);
	  assertAnalyzesToReuse(analyzer, "", new String[] {});

      assertAnalyzesToReuse(
          analyzer,
          "การที่ได้ต้องแสดงว่างานดี",
          new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี"});

      assertAnalyzesToReuse(
          analyzer,
          "บริษัทชื่อ XY&Z - คุยกับ xyz@demo.com",
          new String[] { "บริษัท", "ชื่อ", "xy", "z", "คุย", "กับ", "xyz", "demo.com" });
	}
	
	/** @deprecated, for version back compat */
	@Deprecated
	public void testReusableTokenStream30() throws Exception {
	    assumeTrue("JRE does not support Thai dictionary-based BreakIterator", ThaiWordFilter.DBBI_AVAILABLE);
	    ThaiAnalyzer analyzer = new ThaiAnalyzer(Version.LUCENE_30);
	    assertAnalyzesToReuse(analyzer, "", new String[] {});

	    assertAnalyzesToReuse(
            analyzer,
            "การที่ได้ต้องแสดงว่างานดี",
            new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี"});

	    assertAnalyzesToReuse(
            analyzer,
            "บริษัทชื่อ XY&Z - คุยกับ xyz@demo.com",
            new String[] { "บริษัท", "ชื่อ", "xy&z", "คุย", "กับ", "xyz@demo.com" });
  }
	
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random, new ThaiAnalyzer(TEST_VERSION_CURRENT), 10000*RANDOM_MULTIPLIER);
  }
}

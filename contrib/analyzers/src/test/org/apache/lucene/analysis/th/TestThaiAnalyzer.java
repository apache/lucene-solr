package org.apache.lucene.analysis.th;

/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import junit.framework.TestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

/**
 * Test case for ThaiAnalyzer, modified from TestFrenchAnalyzer
 *
 * @author    Samphan Raruenrom <samphan@osdev.co.th>
 * @version   0.1
 */

public class TestThaiAnalyzer extends TestCase {

	public void assertAnalyzesTo(Analyzer a, String input, String[] output)
		throws Exception {

		TokenStream ts = a.tokenStream("dummy", new StringReader(input));

		for (int i = 0; i < output.length; i++) {
			Token t = ts.next();
			assertNotNull(t);
			assertEquals(t.termText(), output[i]);
		}
		assertNull(ts.next());
		ts.close();
	}

	public void testAnalyzer() throws Exception {
		ThaiAnalyzer analyzer = new ThaiAnalyzer();
	
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
}

package org.apache.solr.spelling;
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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.analysis.Token;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.spelling.PossibilityIterator;
import org.junit.BeforeClass;
import org.junit.Test;

public class SpellPossibilityIteratorTest extends SolrTestCaseJ4 {

	private static Map<Token, LinkedHashMap<String, Integer>> suggestions = new LinkedHashMap<Token, LinkedHashMap<String, Integer>>();

	@BeforeClass
	public static void beforeClass() throws Exception {

		suggestions.clear();

		LinkedHashMap<String, Integer> AYE = new LinkedHashMap<String, Integer>();
		AYE.put("I", 0);
		AYE.put("II", 0);
		AYE.put("III", 0);
		AYE.put("IV", 0);
		AYE.put("V", 0);
		AYE.put("VI", 0);
		AYE.put("VII", 0);
		AYE.put("VIII", 0);
		
		LinkedHashMap<String, Integer> BEE = new LinkedHashMap<String, Integer>();
		BEE.put("alpha", 0);
		BEE.put("beta", 0);
		BEE.put("gamma", 0);
		BEE.put("delta", 0);
		BEE.put("epsilon", 0);
		BEE.put("zeta", 0);
		BEE.put("eta", 0);
		BEE.put("theta", 0);
		BEE.put("iota", 0);
		

		LinkedHashMap<String, Integer> CEE = new LinkedHashMap<String, Integer>();
		CEE.put("one", 0);
		CEE.put("two", 0);
		CEE.put("three", 0);
		CEE.put("four", 0);
		CEE.put("five", 0);
		CEE.put("six", 0);
		CEE.put("seven", 0);
		CEE.put("eight", 0);
		CEE.put("nine", 0);
		CEE.put("ten", 0);

		suggestions.put(new Token("AYE", 0, 2), AYE);
		suggestions.put(new Token("BEE", 0, 2), BEE);
		suggestions.put(new Token("CEE", 0, 2), CEE);
	}

	@Test
	public void testSpellPossibilityIterator() throws Exception {
		PossibilityIterator iter = new PossibilityIterator(suggestions);
		int count = 0;
		while (iter.hasNext()) {
			
			iter.next();
			count++;
		}
		assertTrue(("Three maps (8*9*10) should return 720 iterations but instead returned " + count), count == 720);

		suggestions.remove(new Token("CEE", 0, 2));
		iter = new PossibilityIterator(suggestions);
		count = 0;
		while (iter.hasNext()) {
			iter.next();
			count++;
		}
		assertTrue(("Two maps (8*9) should return 72 iterations but instead returned " + count), count == 72);

		suggestions.remove(new Token("BEE", 0, 2));
		iter = new PossibilityIterator(suggestions);
		count = 0;
		while (iter.hasNext()) {
			iter.next();
			count++;
		}
		assertTrue(("One map of 8 should return 8 iterations but instead returned " + count), count == 8);

		suggestions.remove(new Token("AYE", 0, 2));
		iter = new PossibilityIterator(suggestions);
		count = 0;
		while (iter.hasNext()) {
			iter.next();
			count++;
		}
		assertTrue(("No maps should return 0 iterations but instead returned " + count), count == 0);

	}
}

package org.apache.lucene.analysis.kuromoji.dict;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.lucene.analysis.kuromoji.dict.UnknownDictionary;
import org.apache.lucene.analysis.kuromoji.util.CSVUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class UnknownDictionaryTest extends LuceneTestCase {
	public static final String FILENAME = "unk-tokeninfo-dict.obj";

	@Test
	public void testPutCharacterCategory() {
		UnknownDictionary unkDic = new UnknownDictionary(10 * 1024 * 1024);
		
		try{
			unkDic.putCharacterCategory(0, "DUMMY_NAME");
			fail();
		} catch(Exception e) {
			
		}

		try{
			unkDic.putCharacterCategory(-1, "KATAKANA");
			fail();
		} catch(Exception e) {
			
		}
		
		unkDic.putCharacterCategory(0, "DEFAULT");
		unkDic.putCharacterCategory(1, "GREEK");
		unkDic.putCharacterCategory(2, "HIRAGANA");
		unkDic.putCharacterCategory(3, "KATAKANA");
		unkDic.putCharacterCategory(4, "KANJI");
	}
	
	@Test
	public void testPut() {
		UnknownDictionary unkDic = new UnknownDictionary(10 * 1024 * 1024);
		try{
			unkDic.put(CSVUtil.parse("KANJI,1285,11426,名詞,一般,*,*,*,*,*"));
			fail();
		} catch(Exception e){
			
		}
		
		String entry1 = "KANJI,1285,1285,11426,名詞,一般,*,*,*,*,*";
		String entry2 = "ALPHA,1285,1285,13398,名詞,一般,*,*,*,*,*";
		String entry3 = "HIRAGANA,1285,1285,13069,名詞,一般,*,*,*,*,*";
		
		unkDic.putCharacterCategory(0, "KANJI");
		unkDic.putCharacterCategory(1, "ALPHA");
		unkDic.putCharacterCategory(2, "HIRAGANA");
		
		unkDic.put(CSVUtil.parse(entry1));
		unkDic.put(CSVUtil.parse(entry2));
		unkDic.put(CSVUtil.parse(entry3));
	}
	
	private UnknownDictionary createDictionary() throws IOException {
		InputStream is = this.getClass().getClassLoader().getResourceAsStream("unk.def.utf-8");
		UnknownDictionary dictionary = new UnknownDictionary();
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		
		String line = null;
		while((line = reader.readLine()) != null) {
			dictionary.put(CSVUtil.parse(line));
		}
		reader.close();

		is = this.getClass().getClassLoader().getResourceAsStream("char.def.utf-8");
		reader = new BufferedReader(new InputStreamReader(is));
		
		line = null;
		while ((line = reader.readLine()) != null) {
			line = line.replaceAll("^\\s", "");
			line = line.replaceAll("\\s*#.*", "");
			line = line.replaceAll("\\s+", " ");
			
			// Skip empty line or comment line
			if(line.length() == 0) {
				continue;
			}
			
			if(line.startsWith("0x")) {	// Category mapping
				String[] values = line.split(" ", 2);	// Split only first space
				
				if(!values[0].contains("..")) {
					int cp = Integer.decode(values[0]).intValue();
					dictionary.putCharacterCategory(cp, values[1]);					
				} else {
					String[] codePoints = values[0].split("\\.\\.");
					int cpFrom = Integer.decode(codePoints[0]).intValue();
					int cpTo = Integer.decode(codePoints[1]).intValue();
					
					for(int i = cpFrom; i <= cpTo; i++){
						dictionary.putCharacterCategory(i, values[1]);					
					}
				}
			} else {	// Invoke definition
				String[] values = line.split(" "); // Consecutive space is merged above
				String characterClassName = values[0];
				int invoke = Integer.parseInt(values[1]);
				int group = Integer.parseInt(values[2]);
				int length = Integer.parseInt(values[3]);
				dictionary.putInvokeDefinition(characterClassName, invoke, group, length);
			}
			
		}
		
		reader.close();
		
		return dictionary;
	}
}

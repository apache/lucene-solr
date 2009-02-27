package org.apache.lucene.search.spell;

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

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.English;


/**
 * Spell checker test case
 *
 *
 */
public class TestSpellChecker extends TestCase {
  private SpellChecker spellChecker;
  private Directory userindex, spellindex;

  protected void setUp() throws Exception {
    super.setUp();
    
    //create a user index
    userindex = new RAMDirectory();
    IndexWriter writer = new IndexWriter(userindex, new SimpleAnalyzer(), true);

    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      doc.add(new Field("field1", English.intToEnglish(i), Field.Store.YES, Field.Index.ANALYZED));
      doc.add(new Field("field2", English.intToEnglish(i + 1), Field.Store.YES, Field.Index.ANALYZED)); // + word thousand
      writer.addDocument(doc);
    }
    writer.close();

    // create the spellChecker
    spellindex = new RAMDirectory();
    spellChecker = new SpellChecker(spellindex);
  }


  public void testBuild() throws CorruptIndexException, IOException {
    IndexReader r = IndexReader.open(userindex);

    spellChecker.clearIndex();

    addwords(r, "field1");
    int num_field1 = this.numdoc();

    addwords(r, "field2");
    int num_field2 = this.numdoc();

    assertEquals(num_field2, num_field1 + 1);

    checkCommonSuggestions(r);
    checkLevenshteinSuggestions(r);
    
    spellChecker.setStringDistance(new JaroWinklerDistance());
    spellChecker.setAccuracy(0.8f);
    checkCommonSuggestions(r);
    checkJaroWinklerSuggestions();
  }

  private void checkCommonSuggestions(IndexReader r) throws IOException {
    String[] similar = spellChecker.suggestSimilar("fvie", 2);
    assertTrue(similar.length > 0);
    assertEquals(similar[0], "five");
    
    similar = spellChecker.suggestSimilar("five", 2);
    if (similar.length > 0) {
      assertFalse(similar[0].equals("five")); // don't suggest a word for itself
    }
    
    similar = spellChecker.suggestSimilar("fiv", 2);
    assertTrue(similar.length > 0);
    assertEquals(similar[0], "five");
    
    similar = spellChecker.suggestSimilar("fives", 2);
    assertTrue(similar.length > 0);
    assertEquals(similar[0], "five");
    
    assertTrue(similar.length > 0);
    similar = spellChecker.suggestSimilar("fie", 2);
    assertEquals(similar[0], "five");
    
    //  test restraint to a field
    similar = spellChecker.suggestSimilar("tousand", 10, r, "field1", false);
    assertEquals(0, similar.length); // there isn't the term thousand in the field field1

    similar = spellChecker.suggestSimilar("tousand", 10, r, "field2", false);
    assertEquals(1, similar.length); // there is the term thousand in the field field2
  }

  private void checkLevenshteinSuggestions(IndexReader r) throws IOException {
    // test small word
    String[] similar = spellChecker.suggestSimilar("fvie", 2);
    assertEquals(1, similar.length);
    assertEquals(similar[0], "five");

    similar = spellChecker.suggestSimilar("five", 2);
    assertEquals(1, similar.length);
    assertEquals(similar[0], "nine");     // don't suggest a word for itself

    similar = spellChecker.suggestSimilar("fiv", 2);
    assertEquals(1, similar.length);
    assertEquals(similar[0], "five");

    similar = spellChecker.suggestSimilar("ive", 2);
    assertEquals(2, similar.length);
    assertEquals(similar[0], "five");
    assertEquals(similar[1], "nine");

    similar = spellChecker.suggestSimilar("fives", 2);
    assertEquals(1, similar.length);
    assertEquals(similar[0], "five");

    similar = spellChecker.suggestSimilar("fie", 2);
    assertEquals(2, similar.length);
    assertEquals(similar[0], "five");
    assertEquals(similar[1], "nine");
    
    similar = spellChecker.suggestSimilar("fi", 2);
    assertEquals(1, similar.length);
    assertEquals(similar[0], "five");

    // test restraint to a field
    similar = spellChecker.suggestSimilar("tousand", 10, r, "field1", false);
    assertEquals(0, similar.length); // there isn't the term thousand in the field field1

    similar = spellChecker.suggestSimilar("tousand", 10, r, "field2", false);
    assertEquals(1, similar.length); // there is the term thousand in the field field2
    
    similar = spellChecker.suggestSimilar("onety", 2);
    assertEquals(2, similar.length);
    assertEquals(similar[0], "ninety");
    assertEquals(similar[1], "one");
    try {
      similar = spellChecker.suggestSimilar("tousand", 10, r, null, false);
    } catch (NullPointerException e) {
      assertTrue("threw an NPE, and it shouldn't have", false);
    }
  }

  private void checkJaroWinklerSuggestions() throws IOException {
    String[] similar = spellChecker.suggestSimilar("onety", 2);
    assertEquals(2, similar.length);
    assertEquals(similar[0], "one");
  }

  private void addwords(IndexReader r, String field) throws IOException {
    long time = System.currentTimeMillis();
    spellChecker.indexDictionary(new LuceneDictionary(r, field));
    time = System.currentTimeMillis() - time;
    //System.out.println("time to build " + field + ": " + time);
  }

  private int numdoc() throws IOException {
    IndexReader rs = IndexReader.open(spellindex);
    int num = rs.numDocs();
    assertTrue(num != 0);
    //System.out.println("num docs: " + num);
    rs.close();
    return num;
  }
  
}

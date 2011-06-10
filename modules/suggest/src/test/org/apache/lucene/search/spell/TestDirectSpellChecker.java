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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;

public class TestDirectSpellChecker extends LuceneTestCase {

  public void testSimpleExamples() throws Exception {
    DirectSpellChecker spellChecker = new DirectSpellChecker();
    spellChecker.setMinQueryLength(0);
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, dir, 
        new MockAnalyzer(random, MockTokenizer.SIMPLE, true));

    for (int i = 0; i < 20; i++) {
      Document doc = new Document();
      doc.add(newField("numbers", English.intToEnglish(i), Field.Store.NO, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }

    IndexReader ir = writer.getReader();

    SuggestWord[] similar = spellChecker.suggestSimilar(new Term("numbers", "fvie"), 2, ir, false);
    assertTrue(similar.length > 0);
    assertEquals("five", similar[0].string);

    similar = spellChecker.suggestSimilar(new Term("numbers", "five"), 2, ir, false);
    if (similar.length > 0) {
      assertFalse(similar[0].string.equals("five")); // don't suggest a word for itself
    }

    similar = spellChecker.suggestSimilar(new Term("numbers", "fvie"), 2, ir, false);
    assertTrue(similar.length > 0);
    assertEquals("five", similar[0].string);

    similar = spellChecker.suggestSimilar(new Term("numbers", "fiv"), 2, ir, false);
    assertTrue(similar.length > 0);
    assertEquals("five", similar[0].string);

    similar = spellChecker.suggestSimilar(new Term("numbers", "fives"), 2, ir, false);
    assertTrue(similar.length > 0);
    assertEquals("five", similar[0].string);

    assertTrue(similar.length > 0);
    similar = spellChecker.suggestSimilar(new Term("numbers", "fie"), 2, ir, false);
    assertEquals("five", similar[0].string);

    // add some more documents
    for (int i = 1000; i < 1100; i++) {
      Document doc = new Document();
      doc.add(newField("numbers", English.intToEnglish(i), Field.Store.NO, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }

    ir.close();
    ir = writer.getReader();

    // look ma, no spellcheck index rebuild
    similar = spellChecker.suggestSimilar(new Term("numbers", "tousand"), 10, ir, false);
    assertTrue(similar.length > 0); 
    assertEquals("thousand", similar[0].string);

    ir.close();
    writer.close();
    dir.close();
  }
  
  public void testOptions() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, dir, 
        new MockAnalyzer(random, MockTokenizer.SIMPLE, true));

    Document doc = new Document();
    doc.add(newField("text", "foobar", Field.Store.NO, Field.Index.ANALYZED));
    writer.addDocument(doc);
    doc.add(newField("text", "foobar", Field.Store.NO, Field.Index.ANALYZED));
    writer.addDocument(doc);
    doc.add(newField("text", "foobaz", Field.Store.NO, Field.Index.ANALYZED));
    writer.addDocument(doc);
    doc.add(newField("text", "fobar", Field.Store.NO, Field.Index.ANALYZED));
    writer.addDocument(doc);
   
    IndexReader ir = writer.getReader();
    
    DirectSpellChecker spellChecker = new DirectSpellChecker();
    spellChecker.setMaxQueryFrequency(0F);
    SuggestWord[] similar = spellChecker.suggestSimilar(new Term("text", "fobar"), 1, ir, true);
    assertEquals(0, similar.length);
    
    spellChecker = new DirectSpellChecker(); // reset defaults
    spellChecker.setMinQueryLength(5);
    similar = spellChecker.suggestSimilar(new Term("text", "foba"), 1, ir, true);
    assertEquals(0, similar.length);
    
    spellChecker = new DirectSpellChecker(); // reset defaults
    spellChecker.setMaxEdits(1);
    similar = spellChecker.suggestSimilar(new Term("text", "foobazzz"), 1, ir, true);
    assertEquals(0, similar.length);
    
    spellChecker = new DirectSpellChecker(); // reset defaults
    spellChecker.setAccuracy(0.9F);
    similar = spellChecker.suggestSimilar(new Term("text", "foobazzz"), 1, ir, true);
    assertEquals(0, similar.length);
    
    spellChecker = new DirectSpellChecker(); // reset defaults
    spellChecker.setMinPrefix(0);
    similar = spellChecker.suggestSimilar(new Term("text", "roobaz"), 1, ir, true);
    assertEquals(1, similar.length);
    
    spellChecker = new DirectSpellChecker(); // reset defaults
    spellChecker.setMinPrefix(1);
    similar = spellChecker.suggestSimilar(new Term("text", "roobaz"), 1, ir, true);
    assertEquals(0, similar.length);
    
    ir.close();
    writer.close();
    dir.close();
  }
}

package org.apache.lucene.search.spell;


import junit.framework.*;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.util.English;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;

import org.apache.lucene.store.Directory;


/**
 * Test case
 *
 * @author Nicolas Maisonneuve
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
      doc.add(new Field("field1", English.intToEnglish(i), Field.Store.YES, Field.Index.TOKENIZED));
      doc.add(new Field("field2", English.intToEnglish(i + 1), Field.Store.YES, Field.Index.TOKENIZED)); // + word thousand
      writer.addDocument(doc);
    }
    writer.close();

    // create the spellChecker
    spellindex = new RAMDirectory();
    spellChecker = new SpellChecker(spellindex);
  }


  public void testBuild() {
    try {
      IndexReader r = IndexReader.open(userindex);

      spellChecker.clearIndex();

      addwords(r, "field1");
      int num_field1 = this.numdoc();

      addwords(r, "field2");
      int num_field2 = this.numdoc();

      assertEquals(num_field2, num_field1 + 1);

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
      assertEquals(1, similar.length);
      assertEquals(similar[0], "five");

      similar = spellChecker.suggestSimilar("fives", 2);
      assertEquals(1, similar.length);
      assertEquals(similar[0], "five");

      similar = spellChecker.suggestSimilar("fie", 2);
      assertEquals(1, similar.length);
      assertEquals(similar[0], "five");

      similar = spellChecker.suggestSimilar("fi", 2);
      assertEquals(0, similar.length);

      // test restraint to a field
      similar = spellChecker.suggestSimilar("tousand", 10, r, "field1", false);
      assertEquals(0, similar.length); // there isn't the term thousand in the field field1

      similar = spellChecker.suggestSimilar("tousand", 10, r, "field2", false);
      assertEquals(1, similar.length); // there is the term thousand in the field field2
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
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

  protected void tearDown() throws Exception {
    spellChecker = null;
    super.tearDown();
  }

}

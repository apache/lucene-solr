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
package org.apache.lucene.codecs.blocktreeords;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.BasePostingsFormatTestCase;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;

public class TestOrdsBlockTree extends BasePostingsFormatTestCase {
  private final Codec codec = TestUtil.alwaysPostingsFormat(new BlockTreeOrdsPostingsFormat());

  @Override
  protected Codec getCodec() {
    return codec;
  }

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "a b c", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    TermsEnum te = MultiFields.getTerms(r, "field").iterator();

    // Test next()
    assertEquals(new BytesRef("a"), te.next());
    assertEquals(0L, te.ord());
    assertEquals(new BytesRef("b"), te.next());
    assertEquals(1L, te.ord());
    assertEquals(new BytesRef("c"), te.next());
    assertEquals(2L, te.ord());
    assertNull(te.next());

    // Test seekExact by term
    assertTrue(te.seekExact(new BytesRef("b")));
    assertEquals(1, te.ord());
    assertTrue(te.seekExact(new BytesRef("a")));
    assertEquals(0, te.ord());
    assertTrue(te.seekExact(new BytesRef("c")));
    assertEquals(2, te.ord());

    // Test seekExact by ord
    te.seekExact(1);
    assertEquals(new BytesRef("b"), te.term());
    te.seekExact(0);
    assertEquals(new BytesRef("a"), te.term());
    te.seekExact(2);
    assertEquals(new BytesRef("c"), te.term());

    r.close();
    w.close();
    dir.close();
  }

  public void testTwoBlocks() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    List<String> terms = new ArrayList<>();
    for(int i=0;i<36;i++) {
      Document doc = new Document();
      String term = "" + (char) (97+i);
      terms.add(term);
      if (VERBOSE) {
        System.out.println("i=" + i + " term=" + term);
      }
      doc.add(newTextField("field", term, Field.Store.NO));
      w.addDocument(doc);
    }
    for(int i=0;i<36;i++) {
      Document doc = new Document();
      String term = "m" + (char) (97+i);
      terms.add(term);
      if (VERBOSE) {
        System.out.println("i=" + i + " term=" + term);
      }
      doc.add(newTextField("field", term, Field.Store.NO));
      w.addDocument(doc);
    }
    if (VERBOSE) {
      System.out.println("TEST: now forceMerge");
    }
    w.forceMerge(1);
    IndexReader r = w.getReader();
    TermsEnum te = MultiFields.getTerms(r, "field").iterator();

    assertTrue(te.seekExact(new BytesRef("mo")));
    assertEquals(27, te.ord());

    te.seekExact(54);
    assertEquals(new BytesRef("s"), te.term());

    Collections.sort(terms);

    for(int i=terms.size()-1;i>=0;i--) {
      te.seekExact(i);
      assertEquals(i, te.ord());
      assertEquals(terms.get(i), te.term().utf8ToString());
    }

    int iters = atLeast(1000);
    for(int iter=0;iter<iters;iter++) {
      int ord = random().nextInt(terms.size());
      BytesRef term = new BytesRef(terms.get(ord));
      if (random().nextBoolean()) {
        if (VERBOSE) {
          System.out.println("TEST: iter=" + iter + " seek to ord=" + ord + " of " + terms.size());
        }
        te.seekExact(ord);
      } else {
        if (VERBOSE) {
          System.out.println("TEST: iter=" + iter + " seek to term=" + terms.get(ord) + " ord=" + ord + " of " + terms.size());
        }
        te.seekExact(term);
      }
      assertEquals(ord, te.ord());
      assertEquals(term, te.term());
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testThreeBlocks() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    List<String> terms = new ArrayList<>();
    for(int i=0;i<36;i++) {
      Document doc = new Document();
      String term = "" + (char) (97+i);
      terms.add(term);
      if (VERBOSE) {
        System.out.println("i=" + i + " term=" + term);
      }
      doc.add(newTextField("field", term, Field.Store.NO));
      w.addDocument(doc);
    }
    for(int i=0;i<36;i++) {
      Document doc = new Document();
      String term = "m" + (char) (97+i);
      terms.add(term);
      if (VERBOSE) {
        System.out.println("i=" + i + " term=" + term);
      }
      doc.add(newTextField("field", term, Field.Store.NO));
      w.addDocument(doc);
    }
    for(int i=0;i<36;i++) {
      Document doc = new Document();
      String term = "mo" + (char) (97+i);
      terms.add(term);
      if (VERBOSE) {
        System.out.println("i=" + i + " term=" + term);
      }
      doc.add(newTextField("field", term, Field.Store.NO));
      w.addDocument(doc);
    }
    w.forceMerge(1);
    IndexReader r = w.getReader();
    TermsEnum te = MultiFields.getTerms(r, "field").iterator();

    if (VERBOSE) {
      while (te.next() != null) {
        System.out.println("TERM: " + te.ord() + " " + te.term().utf8ToString());
      }
    }

    assertTrue(te.seekExact(new BytesRef("mo")));
    assertEquals(27, te.ord());

    te.seekExact(90);
    assertEquals(new BytesRef("s"), te.term());

    testEnum(te, terms);

    r.close();
    w.close();
    dir.close();
  }

  private void testEnum(TermsEnum te, List<String> terms) throws IOException {
    Collections.sort(terms);
    for(int i=terms.size()-1;i>=0;i--) {
      if (VERBOSE) {
        System.out.println("TEST: seek to ord=" + i);
      }
      te.seekExact(i);
      assertEquals(i, te.ord());
      assertEquals(terms.get(i), te.term().utf8ToString());
    }

    int iters = atLeast(1000);
    for(int iter=0;iter<iters;iter++) {
      int ord = random().nextInt(terms.size());
      if (random().nextBoolean()) {
        te.seekExact(ord);
        assertEquals(terms.get(ord), te.term().utf8ToString());
      } else {
        te.seekExact(new BytesRef(terms.get(ord)));
        assertEquals(ord, te.ord());
      }
    }
  }

  public void testFloorBlocks() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    for(int i=0;i<128;i++) {
      Document doc = new Document();
      String term = "" + (char) i;
      if (VERBOSE) {
        System.out.println("i=" + i + " term=" + term + " bytes=" + new BytesRef(term));
      }
      doc.add(newStringField("field", term, Field.Store.NO));
      w.addDocument(doc);
    }
    w.forceMerge(1);
    IndexReader r = DirectoryReader.open(w);
    TermsEnum te = MultiFields.getTerms(r, "field").iterator();

    if (VERBOSE) {
      BytesRef term;
      while ((term = te.next()) != null) {
        System.out.println("  " + te.ord() + ": " + term.utf8ToString());
      }
    }

    assertTrue(te.seekExact(new BytesRef("a")));
    assertEquals(97, te.ord());

    te.seekExact(98);
    assertEquals(new BytesRef("b"), te.term());

    assertTrue(te.seekExact(new BytesRef("z")));
    assertEquals(122, te.ord());

    r.close();
    w.close();
    dir.close();
  }

  public void testNonRootFloorBlocks() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    List<String> terms = new ArrayList<>();
    for(int i=0;i<36;i++) {
      Document doc = new Document();
      String term = "" + (char) (97+i);
      terms.add(term);
      if (VERBOSE) {
        System.out.println("i=" + i + " term=" + term);
      }
      doc.add(newTextField("field", term, Field.Store.NO));
      w.addDocument(doc);
    }
    for(int i=0;i<128;i++) {
      Document doc = new Document();
      String term = "m" + (char) i;
      terms.add(term);
      if (VERBOSE) {
        System.out.println("i=" + i + " term=" + term + " bytes=" + new BytesRef(term));
      }
      doc.add(newStringField("field", term, Field.Store.NO));
      w.addDocument(doc);
    }
    w.forceMerge(1);
    IndexReader r = DirectoryReader.open(w);
    TermsEnum te = MultiFields.getTerms(r, "field").iterator();

    BytesRef term;
    int ord = 0;
    while ((term = te.next()) != null) {
      if (VERBOSE) {
        System.out.println("TEST: " + te.ord() + ": " + term.utf8ToString());
      }
      assertEquals(ord, te.ord());
      ord++;
    }

    testEnum(te, terms);

    r.close();
    w.close();
    dir.close();
  }

  public void testSeveralNonRootBlocks() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    List<String> terms = new ArrayList<>();
    for(int i=0;i<30;i++) {
      for(int j=0;j<30;j++) {
        Document doc = new Document();
        String term = "" + (char) (97+i) + (char) (97+j);
        terms.add(term);
        if (VERBOSE) {
          System.out.println("term=" + term);
        }
        doc.add(newTextField("body", term, Field.Store.NO));
        w.addDocument(doc);
      }
    }
    w.forceMerge(1);
    IndexReader r = DirectoryReader.open(w);
    TermsEnum te = MultiFields.getTerms(r, "body").iterator();

    for(int i=0;i<30;i++) {
      for(int j=0;j<30;j++) {
        String term = "" + (char) (97+i) + (char) (97+j);
        if (VERBOSE) {
          System.out.println("TEST: check term=" + term);
        }
        assertEquals(term, te.next().utf8ToString());
        assertEquals(30*i+j, te.ord());
      }
    }

    testEnum(te, terms);

    te.seekExact(0);
    assertEquals("aa", te.term().utf8ToString());

    r.close();
    w.close();
    dir.close();
  }

  public void testSeekCeilNotFound() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    // Get empty string in there!
    doc.add(newStringField("field", "", Field.Store.NO));
    w.addDocument(doc);
    
    for(int i=0;i<36;i++) {
      doc = new Document();
      String term = "" + (char) (97+i);
      String term2 = "a" + (char) (97+i);
      doc.add(newTextField("field", term + " " + term2, Field.Store.NO));
      w.addDocument(doc);
    }

    w.forceMerge(1);
    IndexReader r = w.getReader();
    TermsEnum te = MultiFields.getTerms(r, "field").iterator();
    assertEquals(TermsEnum.SeekStatus.NOT_FOUND, te.seekCeil(new BytesRef(new byte[] {0x22})));
    assertEquals("a", te.term().utf8ToString());
    assertEquals(1L, te.ord());
    r.close();
    w.close();
    dir.close();
  }
}

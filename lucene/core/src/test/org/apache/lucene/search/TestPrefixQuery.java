package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.codecs.autoprefix.AutoPrefixPostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;

/**
 * Tests {@link PrefixQuery} class.
 *
 */
public class TestPrefixQuery extends LuceneTestCase {
  public void testPrefixQuery() throws Exception {
    Directory directory = newDirectory();

    String[] categories = new String[] {"/Computers",
                                        "/Computers/Mac",
                                        "/Computers/Windows"};
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    for (int i = 0; i < categories.length; i++) {
      Document doc = new Document();
      doc.add(newStringField("category", categories[i], Field.Store.YES));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();

    PrefixQuery query = new PrefixQuery(new Term("category", "/Computers"));
    IndexSearcher searcher = newSearcher(reader);
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("All documents in /Computers category and below", 3, hits.length);

    query = new PrefixQuery(new Term("category", "/Computers/Mac"));
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("One in /Computers/Mac", 1, hits.length);

    query = new PrefixQuery(new Term("category", ""));
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("everything", 3, hits.length);
    writer.close();
    reader.close();
    directory.close();
  }

  /** Make sure auto prefix terms are used with PrefixQuery. */
  public void testAutoPrefixTermsKickIn() throws Exception {

    List<String> prefixes = new ArrayList<>();
    for(int i=1;i<5;i++) {
      char[] chars = new char[i];
      Arrays.fill(chars, 'a');
      prefixes.add(new String(chars));
    }

    Set<String> randomTerms = new HashSet<>();
    int numTerms = atLeast(10000);
    while (randomTerms.size() < numTerms) {
      for(String prefix : prefixes) {
        randomTerms.add(prefix + TestUtil.randomRealisticUnicodeString(random()));
      }
    }

    int actualCount = 0;
    for(String term : randomTerms) {
      if (term.startsWith("aa")) {
        actualCount++;
      }
    }

    //System.out.println("actual count " + actualCount);

    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    int minTermsInBlock = TestUtil.nextInt(random(), 2, 100);
    int maxTermsInBlock = Math.max(2, (minTermsInBlock-1)*2 + random().nextInt(100));

    // As long as this is never > actualCount, aa should always see at least one auto-prefix term:
    int minTermsAutoPrefix = TestUtil.nextInt(random(), 2, actualCount);
    int maxTermsAutoPrefix = random().nextBoolean() ? Math.max(2, (minTermsAutoPrefix-1)*2 + random().nextInt(100)) : Integer.MAX_VALUE;

    iwc.setCodec(TestUtil.alwaysPostingsFormat(new AutoPrefixPostingsFormat(minTermsInBlock, maxTermsInBlock,
                                                                            minTermsAutoPrefix, maxTermsAutoPrefix)));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    for (String term : randomTerms) {
      Document doc = new Document();
      doc.add(new StringField("field", term, Field.Store.NO));
      w.addDocument(doc);
    }

    w.forceMerge(1);
    IndexReader r = w.getReader();
    final Terms terms = MultiFields.getTerms(r, "field");
    IndexSearcher s = new IndexSearcher(r);
    final int finalActualCount = actualCount;
    PrefixQuery q = new PrefixQuery(new Term("field", "aa")) {
      public PrefixQuery checkTerms() throws IOException {
        TermsEnum termsEnum = getTermsEnum(terms, new AttributeSource());
        int count = 0;
        while (termsEnum.next() != null) {
          //System.out.println("got term: " + termsEnum.term().utf8ToString());
          count++;
        }

        // Auto-prefix term(s) should have kicked in, so we should have visited fewer than the total number of aa* terms:
        assertTrue(count < finalActualCount);

        return this;
      }
    }.checkTerms();

    int x = BooleanQuery.getMaxClauseCount();
    try {
      BooleanQuery.setMaxClauseCount(randomTerms.size());
      if (random().nextBoolean()) {
        q.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_REWRITE);
      } else if (random().nextBoolean()) {
        q.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE);
      }

      assertEquals(actualCount, s.search(q, 1).totalHits);
    } finally {
      BooleanQuery.setMaxClauseCount(x);
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testMatchAll() throws Exception {
    Directory directory = newDirectory();

    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    doc.add(newStringField("field", "field", Field.Store.YES));
    writer.addDocument(doc);

    IndexReader reader = writer.getReader();

    PrefixQuery query = new PrefixQuery(new Term("field", ""));
    IndexSearcher searcher = newSearcher(reader);

    assertEquals(1, searcher.search(query, 1000).totalHits);
    writer.close();
    reader.close();
    directory.close();
  }

  static final class BinaryTokenStream extends TokenStream {
    private final ByteTermAttribute bytesAtt = addAttribute(ByteTermAttribute.class);
    private boolean available = true;
  
    public BinaryTokenStream(BytesRef bytes) {
      bytesAtt.setBytesRef(bytes);
    }
  
    @Override
    public boolean incrementToken() {
      if (available) {
        clearAttributes();
        available = false;
        return true;
      }
      return false;
    }
  
    @Override
    public void reset() {
      available = true;
    }
  
    public interface ByteTermAttribute extends TermToBytesRefAttribute {
      public void setBytesRef(BytesRef bytes);
    }
  
    public static class ByteTermAttributeImpl extends AttributeImpl implements ByteTermAttribute,TermToBytesRefAttribute {
      private BytesRef bytes;
    
      @Override
      public void fillBytesRef() {
       // no-op: the bytes was already filled by our owner's incrementToken
      }
    
      @Override
      public BytesRef getBytesRef() {
        return bytes;
      }

      @Override
      public void setBytesRef(BytesRef bytes) {
        this.bytes = bytes;
      }
   
      @Override
      public void clear() {}
    
      @Override
      public void copyTo(AttributeImpl target) {
        ByteTermAttributeImpl other = (ByteTermAttributeImpl) target;
        other.bytes = bytes;
      }
    }
  }

  /** Basically a StringField that accepts binary term. */
  private static class BinaryField extends Field {

    final static FieldType TYPE;
    static {
      TYPE = new FieldType(StringField.TYPE_NOT_STORED);
      // Necessary so our custom tokenStream is used by Field.tokenStream:
      TYPE.setTokenized(true);
      TYPE.freeze();
    }

    public BinaryField(String name, BytesRef value) {
      super(name, new BinaryTokenStream(value), TYPE);
    }
  }

  public void testRandomBinaryPrefix() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    int numTerms = atLeast(10000);
    Set<BytesRef> terms = new HashSet<>();
    while (terms.size() < numTerms) {
      byte[] bytes = new byte[TestUtil.nextInt(random(), 1, 10)];
      random().nextBytes(bytes);
      terms.add(new BytesRef(bytes));
    }

    List<BytesRef> termsList = new ArrayList<>(terms);  
    Collections.shuffle(termsList, random());
    for(BytesRef term : termsList) {
      Document doc = new Document();
      doc.add(new BinaryField("field", term));
      w.addDocument(doc);
    }

    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);

    int iters = atLeast(100);   
    for(int iter=0;iter<iters;iter++) {
      byte[] bytes = new byte[random().nextInt(3)];
      random().nextBytes(bytes);
      BytesRef prefix = new BytesRef(bytes);
      PrefixQuery q = new PrefixQuery(new Term("field", prefix));
      int count = 0;
      for(BytesRef term : termsList) {
        if (StringHelper.startsWith(term, prefix)) {
          count++;
        }
      }
      assertEquals(count, s.search(q, 1).totalHits);
    }
    r.close();
    w.close();
    dir.close();
  }
}

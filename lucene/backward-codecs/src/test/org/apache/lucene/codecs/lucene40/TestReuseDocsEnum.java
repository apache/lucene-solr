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
package org.apache.lucene.codecs.lucene40;
import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits.MatchNoBits;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

// TODO: really this should be in BaseTestPF or somewhere else? useful test!
public class TestReuseDocsEnum extends LuceneTestCase {
  
  public void testReuseDocsEnumNoReuse() throws IOException {
    Directory dir = newDirectory();
    Codec cp = TestUtil.alwaysPostingsFormat(new Lucene40RWPostingsFormat());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setCodec(cp));
    int numdocs = atLeast(20);
    createRandomIndex(numdocs, writer, random());
    writer.commit();

    DirectoryReader open = DirectoryReader.open(dir);
    for (LeafReaderContext ctx : open.leaves()) {
      LeafReader indexReader = ctx.reader();
      Terms terms = indexReader.terms("body");
      TermsEnum iterator = terms.iterator();
      IdentityHashMap<PostingsEnum, Boolean> enums = new IdentityHashMap<>();
      while ((iterator.next()) != null) {
        PostingsEnum docs = iterator.postings(null, random().nextBoolean() ? PostingsEnum.FREQS : PostingsEnum.NONE);
        enums.put(docs, true);
      }
      
      assertEquals(terms.size(), enums.size());
    }
    writer.commit();
    IOUtils.close(writer, open, dir);
  }
  
  // tests for reuse only if bits are the same either null or the same instance
  public void testReuseDocsEnumSameBitsOrNull() throws IOException {
    Directory dir = newDirectory();
    Codec cp = TestUtil.alwaysPostingsFormat(new Lucene40RWPostingsFormat());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setCodec(cp));
    int numdocs = atLeast(20);
    createRandomIndex(numdocs, writer, random());
    writer.commit();

    DirectoryReader open = DirectoryReader.open(dir);
    for (LeafReaderContext ctx : open.leaves()) {
      Terms terms = ctx.reader().terms("body");
      TermsEnum iterator = terms.iterator();
      IdentityHashMap<PostingsEnum, Boolean> enums = new IdentityHashMap<>();
      PostingsEnum docs = null;
      while ((iterator.next()) != null) {
        docs = iterator.postings(docs, random().nextBoolean() ? PostingsEnum.FREQS : PostingsEnum.NONE);
        enums.put(docs, true);
      }
      
      assertEquals(1, enums.size());
      
      enums.clear();
      iterator = terms.iterator();
      docs = null;
      while ((iterator.next()) != null) {
        docs = iterator.postings(docs, random().nextBoolean() ? PostingsEnum.FREQS : PostingsEnum.NONE);
        enums.put(docs, true);
      }
      assertEquals(1, enums.size());  
    }
    writer.close();
    IOUtils.close(open, dir);
  }
  
  // make sure we never reuse from another reader even if it is the same field & codec etc
  public void testReuseDocsEnumDifferentReader() throws IOException {
    Directory dir = newDirectory();
    Codec cp = TestUtil.alwaysPostingsFormat(new Lucene40RWPostingsFormat());
    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setMaxTokenLength(TestUtil.nextInt(random(), 1, IndexWriter.MAX_TERM_LENGTH));

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(analyzer).setCodec(cp));
    int numdocs = atLeast(20);
    createRandomIndex(numdocs, writer, random());
    writer.commit();

    DirectoryReader firstReader = DirectoryReader.open(dir);
    DirectoryReader secondReader = DirectoryReader.open(dir);
    List<LeafReaderContext> leaves = firstReader.leaves();
    List<LeafReaderContext> leaves2 = secondReader.leaves();
    
    for (LeafReaderContext ctx : leaves) {
      Terms terms = ctx.reader().terms("body");
      TermsEnum iterator = terms.iterator();
      IdentityHashMap<PostingsEnum, Boolean> enums = new IdentityHashMap<>();
      MatchNoBits bits = new Bits.MatchNoBits(firstReader.maxDoc());
      iterator = terms.iterator();
      PostingsEnum docs = null;
      BytesRef term = null;
      while ((term = iterator.next()) != null) {
        docs = iterator.postings(randomDocsEnum("body", term, leaves2, bits), random().nextBoolean() ? PostingsEnum.FREQS : PostingsEnum.NONE);
        enums.put(docs, true);
      }
      assertEquals(terms.size(), enums.size());
      
      iterator = terms.iterator();
      enums.clear();
      docs = null;
      while ((term = iterator.next()) != null) {
        docs = iterator.postings(randomDocsEnum("body", term, leaves2, bits), random().nextBoolean() ? PostingsEnum.FREQS : PostingsEnum.NONE);
        enums.put(docs, true);
      }
      assertEquals(terms.size(), enums.size());
    }
    writer.close();
    IOUtils.close(firstReader, secondReader, dir);
  }
  
  public PostingsEnum randomDocsEnum(String field, BytesRef term, List<LeafReaderContext> readers, Bits bits) throws IOException {
    if (random().nextInt(10) == 0) {
      return null;
    }
    LeafReader indexReader = readers.get(random().nextInt(readers.size())).reader();
    Terms terms = indexReader.terms(field);
    if (terms == null) {
      return null;
    }
    TermsEnum iterator = terms.iterator();
    if (iterator.seekExact(term)) {
      return iterator.postings(null, random().nextBoolean() ? PostingsEnum.FREQS : PostingsEnum.NONE);
    }
    return null;
  }

  /**
   * populates a writer with random stuff. this must be fully reproducable with
   * the seed!
   */
  public static void createRandomIndex(int numdocs, RandomIndexWriter writer,
      Random random) throws IOException {
    LineFileDocs lineFileDocs = new LineFileDocs(random);

    for (int i = 0; i < numdocs; i++) {
      writer.addDocument(lineFileDocs.nextDoc());
    }
    
    lineFileDocs.close();
  }

}

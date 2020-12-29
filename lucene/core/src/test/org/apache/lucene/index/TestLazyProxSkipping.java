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
package org.apache.lucene.index;

import java.io.IOException;
import org.apache.lucene.analysis.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/** Tests lazy skipping on the proximity file. */
public class TestLazyProxSkipping extends LuceneTestCase {
  private IndexSearcher searcher;
  private int seeksCounter = 0;

  private String field = "tokens";
  private String term1 = "xx";
  private String term2 = "yy";
  private String term3 = "zz";

  private class SeekCountingDirectory extends MockDirectoryWrapper {
    public SeekCountingDirectory(Directory delegate) {
      super(random(), delegate);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      IndexInput ii = super.openInput(name, context);
      if (name.endsWith(".prx") || name.endsWith(".pos")) {
        // we decorate the proxStream with a wrapper class that allows to count the number of calls
        // of seek()
        ii = new SeeksCountingStream(ii);
      }
      return ii;
    }
  }

  private void createIndex(int numHits) throws IOException {
    int numDocs = 500;

    final Analyzer analyzer =
        new Analyzer() {
          @Override
          public TokenStreamComponents createComponents(String fieldName) {
            return new TokenStreamComponents(new MockTokenizer(MockTokenizer.WHITESPACE, true));
          }
        };
    Directory directory = new SeekCountingDirectory(new ByteBuffersDirectory());
    // note: test explicitly disables payloads
    IndexWriter writer =
        new IndexWriter(
            directory,
            newIndexWriterConfig(analyzer)
                .setMaxBufferedDocs(10)
                .setMergePolicy(newLogMergePolicy(false)));

    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      String content;
      if (i % (numDocs / numHits) == 0) {
        // add a document that matches the query "term1 term2"
        content = this.term1 + " " + this.term2;
      } else if (i % 15 == 0) {
        // add a document that only contains term1
        content = this.term1 + " " + this.term1;
      } else {
        // add a document that contains term2 but not term 1
        content = this.term3 + " " + this.term2;
      }

      doc.add(newTextField(this.field, content, Field.Store.YES));
      writer.addDocument(doc);
    }

    // make sure the index has only a single segment
    writer.forceMerge(1);
    writer.close();

    LeafReader reader = getOnlyLeafReader(DirectoryReader.open(directory));

    this.searcher = newSearcher(reader);
  }

  private ScoreDoc[] search() throws IOException {
    // create PhraseQuery "term1 term2" and search
    PhraseQuery pq = new PhraseQuery(field, term1, term2);
    return this.searcher.search(pq, 1000).scoreDocs;
  }

  private void performTest(int numHits) throws IOException {
    createIndex(numHits);
    this.seeksCounter = 0;
    ScoreDoc[] hits = search();
    // verify that the right number of docs was found
    assertEquals(numHits, hits.length);

    // check if the number of calls of seek() does not exceed the number of hits
    assertTrue(this.seeksCounter > 0);
    assertTrue(
        "seeksCounter=" + this.seeksCounter + " numHits=" + numHits,
        this.seeksCounter <= numHits + 1);
    searcher.getIndexReader().close();
  }

  public void testLazySkipping() throws IOException {
    final String fieldFormat = TestUtil.getPostingsFormat(this.field);
    assumeFalse("This test cannot run with Direct postings format", fieldFormat.equals("Direct"));
    assumeFalse(
        "This test cannot run with SimpleText postings format", fieldFormat.equals("SimpleText"));

    // test whether only the minimum amount of seeks()
    // are performed
    performTest(5);
    performTest(10);
  }

  public void testSeek() throws IOException {
    Directory directory = newDirectory();
    IndexWriter writer =
        new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(newTextField(this.field, "a b", Field.Store.YES));
      writer.addDocument(doc);
    }

    writer.close();
    IndexReader reader = DirectoryReader.open(directory);

    PostingsEnum tp = MultiTerms.getTermPostingsEnum(reader, this.field, new BytesRef("b"));

    for (int i = 0; i < 10; i++) {
      tp.nextDoc();
      assertEquals(tp.docID(), i);
      assertEquals(tp.nextPosition(), 1);
    }

    tp = MultiTerms.getTermPostingsEnum(reader, this.field, new BytesRef("a"));

    for (int i = 0; i < 10; i++) {
      tp.nextDoc();
      assertEquals(tp.docID(), i);
      assertEquals(tp.nextPosition(), 0);
    }
    reader.close();
    directory.close();
  }

  // Simply extends IndexInput in a way that we are able to count the number
  // of invocations of seek()
  class SeeksCountingStream extends IndexInput {
    private IndexInput input;

    SeeksCountingStream(IndexInput input) {
      super("SeekCountingStream(" + input + ")");
      this.input = input;
    }

    @Override
    public byte readByte() throws IOException {
      return this.input.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      this.input.readBytes(b, offset, len);
    }

    @Override
    public void close() throws IOException {
      this.input.close();
    }

    @Override
    public long getFilePointer() {
      return this.input.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
      TestLazyProxSkipping.this.seeksCounter++;
      this.input.seek(pos);
    }

    @Override
    public long length() {
      return this.input.length();
    }

    @Override
    public SeeksCountingStream clone() {
      return new SeeksCountingStream(this.input.clone());
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new SeeksCountingStream(this.input.slice(sliceDescription, offset, length));
    }
  }
}

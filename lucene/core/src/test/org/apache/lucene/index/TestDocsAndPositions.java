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
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestDocsAndPositions extends LuceneTestCase {
  private String fieldName;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    fieldName = "field" + random().nextInt();
  }

  /**
   * Simple testcase for {@link PostingsEnum}
   */
  public void testPositionsSimple() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new MockAnalyzer(random())));
    for (int i = 0; i < 39; i++) {
      Document doc = new Document();
      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setOmitNorms(true);
      doc.add(newField(fieldName, "1 2 3 4 5 6 7 8 9 10 "
          + "1 2 3 4 5 6 7 8 9 10 " + "1 2 3 4 5 6 7 8 9 10 "
          + "1 2 3 4 5 6 7 8 9 10", customType));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    writer.close();

    int num = atLeast(13);
    for (int i = 0; i < num; i++) {
      BytesRef bytes = new BytesRef("1");
      IndexReaderContext topReaderContext = reader.getContext();
      for (LeafReaderContext leafReaderContext : topReaderContext.leaves()) {
        PostingsEnum docsAndPosEnum = getDocsAndPositions(
            leafReaderContext.reader(), bytes);
        assertNotNull(docsAndPosEnum);
        if (leafReaderContext.reader().maxDoc() == 0) {
          continue;
        }
        final int advance = docsAndPosEnum.advance(random().nextInt(leafReaderContext.reader().maxDoc()));
        do {
          String msg = "Advanced to: " + advance + " current doc: "
              + docsAndPosEnum.docID(); // TODO: + " usePayloads: " + usePayload;
          assertEquals(msg, 4, docsAndPosEnum.freq());
          assertEquals(msg, 0, docsAndPosEnum.nextPosition());
          assertEquals(msg, 4, docsAndPosEnum.freq());
          assertEquals(msg, 10, docsAndPosEnum.nextPosition());
          assertEquals(msg, 4, docsAndPosEnum.freq());
          assertEquals(msg, 20, docsAndPosEnum.nextPosition());
          assertEquals(msg, 4, docsAndPosEnum.freq());
          assertEquals(msg, 30, docsAndPosEnum.nextPosition());
        } while (docsAndPosEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
      }
    }
    reader.close();
    directory.close();
  }

  public PostingsEnum getDocsAndPositions(LeafReader reader,
      BytesRef bytes) throws IOException {
    Terms terms = reader.terms(fieldName);
    if (terms != null) {
      TermsEnum te = terms.iterator();
      if (te.seekExact(bytes)) {
        return te.postings(null, PostingsEnum.ALL);
      }
    }
    return null;
  }

  /**
   * this test indexes random numbers within a range into a field and checks
   * their occurrences by searching for a number from that range selected at
   * random. All positions for that number are saved up front and compared to
   * the enums positions.
   */
  public void testRandomPositions() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random()))
          .setMergePolicy(newLogMergePolicy()));
    int numDocs = atLeast(47);
    int max = 1051;
    int term = random().nextInt(max);
    Integer[][] positionsInDoc = new Integer[numDocs][];
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setOmitNorms(true);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      ArrayList<Integer> positions = new ArrayList<>();
      StringBuilder builder = new StringBuilder();
      int num = atLeast(131);
      for (int j = 0; j < num; j++) {
        int nextInt = random().nextInt(max);
        builder.append(nextInt).append(" ");
        if (nextInt == term) {
          positions.add(Integer.valueOf(j));
        }
      }
      if (positions.size() == 0) {
        builder.append(term);
        positions.add(num);
      }
      doc.add(newField(fieldName, builder.toString(), customType));
      positionsInDoc[i] = positions.toArray(new Integer[0]);
      writer.addDocument(doc);
    }

    IndexReader reader = writer.getReader();
    writer.close();

    int num = atLeast(13);
    for (int i = 0; i < num; i++) {
      BytesRef bytes = new BytesRef("" + term);
      IndexReaderContext topReaderContext = reader.getContext();
      for (LeafReaderContext leafReaderContext : topReaderContext.leaves()) {
        PostingsEnum docsAndPosEnum = getDocsAndPositions(
            leafReaderContext.reader(), bytes);
        assertNotNull(docsAndPosEnum);
        int initDoc = 0;
        int maxDoc = leafReaderContext.reader().maxDoc();
        // initially advance or do next doc
        if (random().nextBoolean()) {
          initDoc = docsAndPosEnum.nextDoc();
        } else {
          initDoc = docsAndPosEnum.advance(random().nextInt(maxDoc));
        }
        // now run through the scorer and check if all positions are there...
        do {
          int docID = docsAndPosEnum.docID();
          if (docID == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
          Integer[] pos = positionsInDoc[leafReaderContext.docBase + docID];
          assertEquals(pos.length, docsAndPosEnum.freq());
          // number of positions read should be random - don't read all of them
          // allways
          final int howMany = random().nextInt(20) == 0 ? pos.length
              - random().nextInt(pos.length) : pos.length;
          for (int j = 0; j < howMany; j++) {
            assertEquals("iteration: " + i + " initDoc: " + initDoc + " doc: "
                + docID + " base: " + leafReaderContext.docBase
                + " positions: " + Arrays.toString(pos) /* TODO: + " usePayloads: "
                + usePayload*/, pos[j].intValue(), docsAndPosEnum.nextPosition());
          }

          if (random().nextInt(10) == 0) { // once is a while advance
            if (docsAndPosEnum.advance(docID + 1 + random().nextInt((maxDoc - docID))) == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            }
          }

        } while (docsAndPosEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
      }

    }
    reader.close();
    dir.close();
  }

  public void testRandomDocs() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir,
                                                     newIndexWriterConfig(new MockAnalyzer(random()))
                                                       .setMergePolicy(newLogMergePolicy()));
    int numDocs = atLeast(49);
    int max = 15678;
    int term = random().nextInt(max);
    int[] freqInDoc = new int[numDocs];
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setOmitNorms(true);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      StringBuilder builder = new StringBuilder();
      for (int j = 0; j < 199; j++) {
        int nextInt = random().nextInt(max);
        builder.append(nextInt).append(' ');
        if (nextInt == term) {
          freqInDoc[i]++;
        }
      }
      doc.add(newField(fieldName, builder.toString(), customType));
      writer.addDocument(doc);
    }

    IndexReader reader = writer.getReader();
    writer.close();

    int num = atLeast(13);
    for (int i = 0; i < num; i++) {
      BytesRef bytes = new BytesRef("" + term);
      IndexReaderContext topReaderContext = reader.getContext();
      for (LeafReaderContext context : topReaderContext.leaves()) {
        int maxDoc = context.reader().maxDoc();
        PostingsEnum postingsEnum = TestUtil.docs(random(), context.reader(), fieldName, bytes, null, PostingsEnum.FREQS);
        if (findNext(freqInDoc, context.docBase, context.docBase + maxDoc) == Integer.MAX_VALUE) {
          assertNull(postingsEnum);
          continue;
        }
        assertNotNull(postingsEnum);
        postingsEnum.nextDoc();
        for (int j = 0; j < maxDoc; j++) {
          if (freqInDoc[context.docBase + j] != 0) {
            assertEquals(j, postingsEnum.docID());
            assertEquals(postingsEnum.freq(), freqInDoc[context.docBase +j]);
            if (i % 2 == 0 && random().nextInt(10) == 0) {
              int next = findNext(freqInDoc, context.docBase+j+1, context.docBase + maxDoc) - context.docBase;
              int advancedTo = postingsEnum.advance(next);
              if (next >= maxDoc) {
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, advancedTo);
              } else {
                assertTrue("advanced to: " +advancedTo + " but should be <= " + next, next >= advancedTo);  
              }
            } else {
              postingsEnum.nextDoc();
            }
          } 
        }
        assertEquals("docBase: " + context.docBase + " maxDoc: " + maxDoc + " " + postingsEnum.getClass(), DocIdSetIterator.NO_MORE_DOCS, postingsEnum.docID());
      }
      
    }

    reader.close();
    dir.close();
  }
  
  private static int findNext(int[] docs, int pos, int max) {
    for (int i = pos; i < max; i++) {
      if( docs[i] != 0) {
        return i;
      }
    }
    return Integer.MAX_VALUE;
  }

  /**
   * tests retrieval of positions for terms that have a large number of
   * occurrences to force test of buffer refill during positions iteration.
   */
  public void testLargeNumberOfPositions() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig(new MockAnalyzer(random())));
    int howMany = 1000;
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setOmitNorms(true);
    for (int i = 0; i < 39; i++) {
      Document doc = new Document();
      StringBuilder builder = new StringBuilder();
      for (int j = 0; j < howMany; j++) {
        if (j % 2 == 0) {
          builder.append("even ");
        } else {
          builder.append("odd ");
        }
      }
      doc.add(newField(fieldName, builder.toString(), customType));
      writer.addDocument(doc);
    }

    // now do searches
    IndexReader reader = writer.getReader();
    writer.close();

    int num = atLeast(13);
    for (int i = 0; i < num; i++) {
      BytesRef bytes = new BytesRef("even");

      IndexReaderContext topReaderContext = reader.getContext();
      for (LeafReaderContext leafReaderContext : topReaderContext.leaves()) {
        PostingsEnum docsAndPosEnum = getDocsAndPositions(
            leafReaderContext.reader(), bytes);
        assertNotNull(docsAndPosEnum);

        int initDoc = 0;
        int maxDoc = leafReaderContext.reader().maxDoc();
        // initially advance or do next doc
        if (random().nextBoolean()) {
          initDoc = docsAndPosEnum.nextDoc();
        } else {
          initDoc = docsAndPosEnum.advance(random().nextInt(maxDoc));
        }
        String msg = "Iteration: " + i + " initDoc: " + initDoc; // TODO: + " payloads: " + usePayload;
        assertEquals(howMany / 2, docsAndPosEnum.freq());
        for (int j = 0; j < howMany; j += 2) {
          assertEquals("position missmatch index: " + j + " with freq: "
              + docsAndPosEnum.freq() + " -- " + msg, j,
              docsAndPosEnum.nextPosition());
        }
      }
    }
    reader.close();
    dir.close();
  }
  
  public void testDocsEnumStart() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("foo", "bar", Field.Store.NO));
    writer.addDocument(doc);
    DirectoryReader reader = writer.getReader();
    LeafReader r = getOnlySegmentReader(reader);
    PostingsEnum disi = TestUtil.docs(random(), r, "foo", new BytesRef("bar"), null, PostingsEnum.NONE);
    int docid = disi.docID();
    assertEquals(-1, docid);
    assertTrue(disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    
    // now reuse and check again
    TermsEnum te = r.terms("foo").iterator();
    assertTrue(te.seekExact(new BytesRef("bar")));
    disi = TestUtil.docs(random(), te, disi, PostingsEnum.NONE);
    docid = disi.docID();
    assertEquals(-1, docid);
    assertTrue(disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    writer.close();
    r.close();
    dir.close();
  }
  
  public void testDocsAndPositionsEnumStart() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("foo", "bar", Field.Store.NO));
    writer.addDocument(doc);
    DirectoryReader reader = writer.getReader();
    LeafReader r = getOnlySegmentReader(reader);
    PostingsEnum disi = r.postings(new Term("foo", "bar"), PostingsEnum.ALL);
    int docid = disi.docID();
    assertEquals(-1, docid);
    assertTrue(disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    
    // now reuse and check again
    TermsEnum te = r.terms("foo").iterator();
    assertTrue(te.seekExact(new BytesRef("bar")));
    disi = te.postings(disi, PostingsEnum.ALL);
    docid = disi.docID();
    assertEquals(-1, docid);
    assertTrue(disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    writer.close();
    r.close();
    dir.close();
  }
}

package org.apache.lucene.codecs.lucene3x;

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
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.codecs.FieldInfosReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestTermInfosReaderIndex extends LuceneTestCase {
  
  private static int NUMBER_OF_DOCUMENTS;
  private static int NUMBER_OF_FIELDS;
  private static TermInfosReaderIndex index;
  private static Directory directory;
  private static SegmentTermEnum termEnum;
  private static int indexDivisor;
  private static int termIndexInterval;
  private static IndexReader reader;
  private static List<Term> sampleTerms;
  
  /** we will manually instantiate preflex-rw here */
  @BeforeClass
  public static void beforeClass() throws Exception {
    LuceneTestCase.PREFLEX_IMPERSONATION_IS_ACTIVE = true;
    IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT, 
        new MockAnalyzer(random(), MockTokenizer.KEYWORD, false));
    
    termIndexInterval = config.getTermIndexInterval();
    indexDivisor = _TestUtil.nextInt(random(), 1, 10);
    NUMBER_OF_DOCUMENTS = atLeast(100);
    NUMBER_OF_FIELDS = atLeast(Math.max(10, 3*termIndexInterval*indexDivisor/NUMBER_OF_DOCUMENTS));
    
    directory = newDirectory();

    config.setCodec(new PreFlexRWCodec());
    LogMergePolicy mp = newLogMergePolicy();
    // turn off compound file, this test will open some index files directly.
    mp.setUseCompoundFile(false);
    config.setMergePolicy(mp);

    
    populate(directory, config);

    DirectoryReader r0 = IndexReader.open(directory);
    SegmentReader r = LuceneTestCase.getOnlySegmentReader(r0);
    String segment = r.getSegmentName();
    r.close();

    FieldInfosReader infosReader = new PreFlexRWCodec().fieldInfosFormat().getFieldInfosReader();
    FieldInfos fieldInfos = infosReader.read(directory, segment, IOContext.READONCE);
    String segmentFileName = IndexFileNames.segmentFileName(segment, "", Lucene3xPostingsFormat.TERMS_INDEX_EXTENSION);
    long tiiFileLength = directory.fileLength(segmentFileName);
    IndexInput input = directory.openInput(segmentFileName, newIOContext(random()));
    termEnum = new SegmentTermEnum(directory.openInput(IndexFileNames.segmentFileName(segment, "", Lucene3xPostingsFormat.TERMS_EXTENSION), newIOContext(random())), fieldInfos, false);
    int totalIndexInterval = termEnum.indexInterval * indexDivisor;
    
    SegmentTermEnum indexEnum = new SegmentTermEnum(input, fieldInfos, true);
    index = new TermInfosReaderIndex(indexEnum, indexDivisor, tiiFileLength, totalIndexInterval);
    indexEnum.close();
    input.close();
    
    reader = IndexReader.open(directory);
    sampleTerms = sample(random(),reader,1000);
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    termEnum.close();
    reader.close();
    directory.close();
    termEnum = null;
    reader = null;
    directory = null;
    index = null;
    sampleTerms = null;
  }
  
  public void testSeekEnum() throws CorruptIndexException, IOException {
    int indexPosition = 3;
    SegmentTermEnum clone = termEnum.clone();
    Term term = findTermThatWouldBeAtIndex(clone, indexPosition);
    SegmentTermEnum enumerator = clone;
    index.seekEnum(enumerator, indexPosition);
    assertEquals(term, enumerator.term());
    clone.close();
  }
  
  public void testCompareTo() throws IOException {
    Term term = new Term("field" + random().nextInt(NUMBER_OF_FIELDS) ,getText());
    for (int i = 0; i < index.length(); i++) {
      Term t = index.getTerm(i);
      int compareTo = term.compareTo(t);
      assertEquals(compareTo, index.compareTo(term, i));
    }
  }
  
  public void testRandomSearchPerformance() throws CorruptIndexException, IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    for (Term t : sampleTerms) {
      TermQuery query = new TermQuery(t);
      TopDocs topDocs = searcher.search(query, 10);
      assertTrue(topDocs.totalHits > 0);
    }
  }

  private static List<Term> sample(Random random, IndexReader reader, int size) throws IOException {
    List<Term> sample = new ArrayList<Term>();
    Fields fields = MultiFields.getFields(reader);
    for (String field : fields) {
      Terms terms = fields.terms(field);
      assertNotNull(terms);
      TermsEnum termsEnum = terms.iterator(null);
      while (termsEnum.next() != null) {
        if (sample.size() >= size) {
          int pos = random.nextInt(size);
          sample.set(pos, new Term(field, termsEnum.term()));
        } else {
          sample.add(new Term(field, termsEnum.term()));
        }
      }
    }
    Collections.shuffle(sample);
    return sample;
  }

  private Term findTermThatWouldBeAtIndex(SegmentTermEnum termEnum, int index) throws IOException {
    int termPosition = index * termIndexInterval * indexDivisor;
    for (int i = 0; i < termPosition; i++) {
      // TODO: this test just uses random terms, so this is always possible
      assumeTrue("ran out of terms", termEnum.next());
    }
    final Term term = termEnum.term();
    // An indexed term is only written when the term after
    // it exists, so, if the number of terms is 0 mod
    // termIndexInterval, the last index term will not be
    // written; so we require a term after this term
    // as well:
    assumeTrue("ran out of terms", termEnum.next());
    return term;
  }

  private static void populate(Directory directory, IndexWriterConfig config) throws CorruptIndexException, LockObtainFailedException, IOException {
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, config);
    for (int i = 0; i < NUMBER_OF_DOCUMENTS; i++) {
      Document document = new Document();
      for (int f = 0; f < NUMBER_OF_FIELDS; f++) {
        document.add(newStringField("field" + f, getText(), Field.Store.NO));
      }
      writer.addDocument(document);
    }
    writer.forceMerge(1);
    writer.close();
  }

  private static String getText() {
    return Long.toString(random().nextLong(),Character.MAX_RADIX);
  }
}

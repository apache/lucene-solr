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


import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;

import java.io.IOException;
import java.util.Random;

/**
 * Compares one codec against another
 */
@Slow
public class TestDuelingCodecs extends LuceneTestCase {
  Directory leftDir;
  IndexReader leftReader;
  Codec leftCodec;

  Directory rightDir;
  IndexReader rightReader;
  Codec rightCodec;
  RandomIndexWriter leftWriter;
  RandomIndexWriter rightWriter;
  long seed;
  String info;  // for debugging

  @Override
  public void setUp() throws Exception {
    super.setUp();

    // for now it's SimpleText vs Default(random postings format)
    // as this gives the best overall coverage. when we have more
    // codecs we should probably pick 2 from Codec.availableCodecs()
    
    leftCodec = Codec.forName("SimpleText");
    rightCodec = new RandomCodec(random());

    leftDir = newFSDirectory(createTempDir("leftDir"));
    rightDir = newFSDirectory(createTempDir("rightDir"));

    seed = random().nextLong();

    // must use same seed because of random payloads, etc
    int maxTermLength = TestUtil.nextInt(random(), 1, IndexWriter.MAX_TERM_LENGTH);
    MockAnalyzer leftAnalyzer = new MockAnalyzer(new Random(seed));
    leftAnalyzer.setMaxTokenLength(maxTermLength);
    MockAnalyzer rightAnalyzer = new MockAnalyzer(new Random(seed));
    rightAnalyzer.setMaxTokenLength(maxTermLength);

    // but these can be different
    // TODO: this turns this into a really big test of Multi*, is that what we want?
    IndexWriterConfig leftConfig = newIndexWriterConfig(leftAnalyzer);
    leftConfig.setCodec(leftCodec);
    // preserve docids
    leftConfig.setMergePolicy(newLogMergePolicy());

    IndexWriterConfig rightConfig = newIndexWriterConfig(rightAnalyzer);
    rightConfig.setCodec(rightCodec);
    // preserve docids
    rightConfig.setMergePolicy(newLogMergePolicy());

    // must use same seed because of random docvalues fields, etc
    leftWriter = new RandomIndexWriter(new Random(seed), leftDir, leftConfig);
    rightWriter = new RandomIndexWriter(new Random(seed), rightDir, rightConfig);

    info = "left: " + leftCodec.toString() + " / right: " + rightCodec.toString();
  }
  
  @Override
  public void tearDown() throws Exception {
    IOUtils.close(leftWriter,
                  rightWriter,
                  leftReader,
                  rightReader,
                  leftDir,
                  rightDir);
    super.tearDown();
  }

  /**
   * populates a writer with random stuff. this must be fully reproducable with the seed!
   */
  public static void createRandomIndex(int numdocs, RandomIndexWriter writer, long seed) throws IOException {
    Random random = new Random(seed);
    // primary source for our data is from linefiledocs, it's realistic.
    LineFileDocs lineFileDocs = new LineFileDocs(random);

    // TODO: we should add other fields that use things like docs&freqs but omit positions,
    // because linefiledocs doesn't cover all the possibilities.
    for (int i = 0; i < numdocs; i++) {
      Document document = lineFileDocs.nextDoc();
      // grab the title and add some SortedSet instances for fun
      String title = document.get("titleTokenized");
      String split[] = title.split("\\s+");
      document.removeFields("sortedset");
      for (String trash : split) {
        document.add(new SortedSetDocValuesField("sortedset", new BytesRef(trash)));
      }
      // add a numeric dv field sometimes
      document.removeFields("sparsenumeric");
      if (random.nextInt(4) == 2) {
        document.add(new NumericDocValuesField("sparsenumeric", random.nextInt()));
      }
      // add sortednumeric sometimes
      document.removeFields("sparsesortednum");
      if (random.nextInt(5) == 1) {
        document.add(new SortedNumericDocValuesField("sparsesortednum", random.nextLong()));
        if (random.nextBoolean()) {
          document.add(new SortedNumericDocValuesField("sparsesortednum", random.nextLong()));
        }
      }
      writer.addDocument(document);
    }
    
    lineFileDocs.close();
  }
  
  /**
   * checks the two indexes are equivalent
   */
  // we use a small amount of docs here, so it works with any codec 
  public void testEquals() throws IOException {
    int numdocs = atLeast(100);
    createRandomIndex(numdocs, leftWriter, seed);
    createRandomIndex(numdocs, rightWriter, seed);

    leftReader = leftWriter.getReader();
    rightReader = rightWriter.getReader();
    
    assertReaderEquals(info, leftReader, rightReader);
  }

  public void testCrazyReaderEquals() throws IOException {
    int numdocs = atLeast(100);
    createRandomIndex(numdocs, leftWriter, seed);
    createRandomIndex(numdocs, rightWriter, seed);

    leftReader = wrapReader(leftWriter.getReader());
    rightReader = wrapReader(rightWriter.getReader());
    
    // check that our readers are valid
    TestUtil.checkReader(leftReader);
    TestUtil.checkReader(rightReader);
    
    assertReaderEquals(info, leftReader, rightReader);
  }
}

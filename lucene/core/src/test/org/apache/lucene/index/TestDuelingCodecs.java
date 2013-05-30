package org.apache.lucene.index;

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
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Compares one codec against another
 */
public class TestDuelingCodecs extends LuceneTestCase {
  private Directory leftDir;
  private IndexReader leftReader;
  private Codec leftCodec;

  private Directory rightDir;
  private IndexReader rightReader;
  private Codec rightCodec;
  
  private String info;  // for debugging

  @Override
  public void setUp() throws Exception {
    super.setUp();

    // for now its SimpleText vs Lucene42(random postings format)
    // as this gives the best overall coverage. when we have more
    // codecs we should probably pick 2 from Codec.availableCodecs()
    
    leftCodec = Codec.forName("SimpleText");
    rightCodec = new RandomCodec(random());

    leftDir = newDirectory();
    rightDir = newDirectory();

    long seed = random().nextLong();

    // must use same seed because of random payloads, etc
    Analyzer leftAnalyzer = new MockAnalyzer(new Random(seed));
    Analyzer rightAnalyzer = new MockAnalyzer(new Random(seed));
    
    // but these can be different
    // TODO: this turns this into a really big test of Multi*, is that what we want?
    IndexWriterConfig leftConfig = newIndexWriterConfig(TEST_VERSION_CURRENT, leftAnalyzer);
    leftConfig.setCodec(leftCodec);
    // preserve docids
    leftConfig.setMergePolicy(newLogMergePolicy());

    IndexWriterConfig rightConfig = newIndexWriterConfig(TEST_VERSION_CURRENT, rightAnalyzer);
    rightConfig.setCodec(rightCodec);
    // preserve docids
    rightConfig.setMergePolicy(newLogMergePolicy());

    // must use same seed because of random docvalues fields, etc
    RandomIndexWriter leftWriter = new RandomIndexWriter(new Random(seed), leftDir, leftConfig);
    RandomIndexWriter rightWriter = new RandomIndexWriter(new Random(seed), rightDir, rightConfig);
    
    int numdocs = atLeast(100);
    createRandomIndex(numdocs, leftWriter, seed);
    createRandomIndex(numdocs, rightWriter, seed);

    leftReader = maybeWrapReader(leftWriter.getReader());
    leftWriter.close();
    rightReader = maybeWrapReader(rightWriter.getReader());
    rightWriter.close();
    
    // check that our readers are valid
    _TestUtil.checkReader(leftReader);
    _TestUtil.checkReader(rightReader);
    
    info = "left: " + leftCodec.toString() + " / right: " + rightCodec.toString();
  }
  
  @Override
  public void tearDown() throws Exception {
    if (leftReader != null) {
      leftReader.close();
    }
    if (rightReader != null) {
      rightReader.close();   
    }

    if (leftDir != null) {
      leftDir.close();
    }
    if (rightDir != null) {
      rightDir.close();
    }
    
    super.tearDown();
  }
  
  /**
   * populates a writer with random stuff. this must be fully reproducable with the seed!
   */
  public static void createRandomIndex(int numdocs, RandomIndexWriter writer, long seed) throws IOException {
    Random random = new Random(seed);
    // primary source for our data is from linefiledocs, its realistic.
    LineFileDocs lineFileDocs = new LineFileDocs(random);

    // TODO: we should add other fields that use things like docs&freqs but omit positions,
    // because linefiledocs doesn't cover all the possibilities.
    for (int i = 0; i < numdocs; i++) {
      Document document = lineFileDocs.nextDoc();
      // grab the title and add some SortedSet instances for fun
      String title = document.get("titleTokenized");
      String split[] = title.split("\\s+");
      for (String trash : split) {
        document.add(new SortedSetDocValuesField("sortedset", new BytesRef(trash)));
      }
      writer.addDocument(document);
    }
    
    lineFileDocs.close();
  }
  
  /**
   * checks the two indexes are equivalent
   */
  public void testEquals() throws IOException {
    assertReaderEquals(info, leftReader, rightReader);
  }

}

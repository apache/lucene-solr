package org.apache.lucene.index;

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

import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LuceneTestCase;

public class TestAllZerosSegmentsFile extends LuceneTestCase {

  public void test() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random, dir);
    w.addDocument(new Document());
    w.close();

    String nextSegmentsFile = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                                    "",
                                                                    SegmentInfos.getCurrentSegmentGeneration(dir)+1);
    IndexOutput out = dir.createOutput(nextSegmentsFile);
    for(int idx=0;idx<8;idx++) {
      out.writeByte((byte) 0);
    }
    out.close();

    IndexReader r= IndexReader.open(dir,true);
    assertEquals(r.numDocs(), 1);
    r.close();
    dir.close();
  }
}

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
package org.apache.lucene.util.bkd;

import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LuceneTestCase.Monster;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TimeUnits;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

// e.g. run like this: ant test -Dtestcase=Test2BBKDPoints -Dtests.nightly=true -Dtests.verbose=true -Dtests.monster=true
// 
//   or: python -u /l/util/src/python/repeatLuceneTest.py -heap 4g -once -nolog -tmpDir /b/tmp -logDir /l/logs Test2BBKDPoints.test2D -verbose

@TimeoutSuite(millis = 365 * 24 * TimeUnits.HOUR) // hopefully ~1 year is long enough ;)
@Monster("takes at least 4 hours and consumes many GB of temp disk space")
public class Test2BBKDPoints extends LuceneTestCase {
  public void test1D() throws Exception {
    Directory dir = FSDirectory.open(createTempDir("2BBKDPoints1D"));

    final int numDocs = (Integer.MAX_VALUE / 26) + 100;

    BKDWriter w = new BKDWriter(numDocs, dir, "_0", new BKDConfig(1, 1, Long.BYTES, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE),
                                BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP, 26L * numDocs);
    int counter = 0;
    byte[] packedBytes = new byte[Long.BYTES];
    for (int docID = 0; docID < numDocs; docID++) {
      for (int j=0;j<26;j++) {
        // first a random int:
        NumericUtils.intToSortableBytes(random().nextInt(), packedBytes, 0);
        // then our counter, which will overflow a bit in the end:
        NumericUtils.intToSortableBytes(counter, packedBytes, Integer.BYTES);
        w.add(packedBytes, docID);
        counter++;
      }
      if (VERBOSE && docID % 100000 == 0) {
        System.out.println(docID + " of " + numDocs + "...");
      }
    }
    IndexOutput out = dir.createOutput("1d.bkd", IOContext.DEFAULT);
    Runnable finalizer = w.finish(out, out, out);
    long indexFP = out.getFilePointer();
    finalizer.run();
    out.close();

    IndexInput in = dir.openInput("1d.bkd", IOContext.DEFAULT);
    in.seek(indexFP);
    BKDReader r = new BKDReader(in, in, in);
    CheckIndex.VerifyPointsVisitor visitor = new CheckIndex.VerifyPointsVisitor("1d", numDocs, r);
    r.intersect(visitor);
    assertEquals(r.size(), visitor.getPointCountSeen());
    assertEquals(r.getDocCount(), visitor.getDocCountSeen());
    in.close();
    dir.close();
  }

  public void test2D() throws Exception {
    Directory dir = FSDirectory.open(createTempDir("2BBKDPoints2D"));

    final int numDocs = (Integer.MAX_VALUE / 26) + 100;

    BKDWriter w = new BKDWriter(numDocs, dir, "_0", new BKDConfig(2, 2, Long.BYTES, BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE),
                                BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP, 26L * numDocs);
    int counter = 0;
    byte[] packedBytes = new byte[2*Long.BYTES];
    for (int docID = 0; docID < numDocs; docID++) {
      for (int j=0;j<26;j++) {
        // first a random int:
        NumericUtils.intToSortableBytes(random().nextInt(), packedBytes, 0);
        // then our counter, which will overflow a bit in the end:
        NumericUtils.intToSortableBytes(counter, packedBytes, Integer.BYTES);
        // then two random ints for the 2nd dimension:
        NumericUtils.intToSortableBytes(random().nextInt(), packedBytes, Long.BYTES);
        NumericUtils.intToSortableBytes(random().nextInt(), packedBytes, Long.BYTES + Integer.BYTES);
        w.add(packedBytes, docID);
        counter++;
      }
      if (VERBOSE && docID % 100000 == 0) {
        System.out.println(docID + " of " + numDocs + "...");
      }
    }
    IndexOutput out = dir.createOutput("2d.bkd", IOContext.DEFAULT);
    Runnable finalizer = w.finish(out, out, out);
    long indexFP = out.getFilePointer();
    finalizer.run();
    out.close();

    IndexInput in = dir.openInput("2d.bkd", IOContext.DEFAULT);
    in.seek(indexFP);
    BKDReader r = new BKDReader(in, in, in);
    CheckIndex.VerifyPointsVisitor visitor = new CheckIndex.VerifyPointsVisitor("2d", numDocs, r);
    r.intersect(visitor);
    assertEquals(r.size(), visitor.getPointCountSeen());
    assertEquals(r.getDocCount(), visitor.getDocCountSeen());
    in.close();
    dir.close();
  }
}

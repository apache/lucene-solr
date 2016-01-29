package org.apache.lucene.codecs.lucene60;

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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointFormat;
import org.apache.lucene.codecs.PointReader;
import org.apache.lucene.codecs.PointWriter;
import org.apache.lucene.index.BasePointFormatTestCase;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.TestUtil;

/**
 * Tests Lucene60PointFormat
 */
public class TestLucene60PointFormat extends BasePointFormatTestCase {
  private final Codec codec;
  
  public TestLucene60PointFormat() {
    // standard issue
    Codec defaultCodec = TestUtil.getDefaultCodec();
    if (random().nextBoolean()) {
      // randomize parameters
      int maxPointsInLeafNode = TestUtil.nextInt(random(), 50, 500);
      double maxMBSortInHeap = 0.1 + (3*random().nextDouble());
      if (VERBOSE) {
        System.out.println("TEST: using Lucene60PointFormat with maxPointsInLeafNode=" + maxPointsInLeafNode + " and maxMBSortInHeap=" + maxMBSortInHeap);
      }

      // sneaky impersonation!
      codec = new FilterCodec(defaultCodec.getName(), defaultCodec) {
        @Override
        public PointFormat pointFormat() {
          return new PointFormat() {
            @Override
            public PointWriter fieldsWriter(SegmentWriteState writeState) throws IOException {
              return new Lucene60PointWriter(writeState, maxPointsInLeafNode, maxMBSortInHeap);
            }

            @Override
            public PointReader fieldsReader(SegmentReadState readState) throws IOException {
              return new Lucene60PointReader(readState);
            }
          };
        }
      };
    } else {
      // standard issue
      codec = defaultCodec;
    }
  }

  @Override
  protected Codec getCodec() {
    return codec;
  }

  @Override
  public void testMergeStability() throws Exception {
    assumeFalse("TODO: mess with the parameters and test gets angry!", codec instanceof FilterCodec);
    super.testMergeStability();
  }
  
}

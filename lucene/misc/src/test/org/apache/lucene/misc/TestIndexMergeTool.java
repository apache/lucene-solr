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
package org.apache.lucene.misc;

import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.misc.IndexMergeTool.Options;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.PrintStreamInfoStream;

public class TestIndexMergeTool extends LuceneTestCase {

  public void testNoParameters() throws Exception {
    expectThrows(IllegalArgumentException.class, () -> {
      Options.parse(new String[] {});
    });
  }

  public void testOneParameter() throws Exception {
    expectThrows(IllegalArgumentException.class, () -> {
      Options.parse(new String[] { "target" });
    });
  }

  public void testTwoParameters() throws Exception {
    expectThrows(IllegalArgumentException.class, () -> {
      Options.parse(new String[] { "target", "source1" });
    });
  }

  public void testThreeParameters() throws Exception {
    Options options = Options.parse(new String[] { "target", "source1", "source2" });
    assertEquals("target", options.mergedIndexPath);
    assertArrayEquals(new String[] { "source1", "source2" }, options.indexPaths);
  }

  public void testVerboseOption() throws Exception {
    Options options = Options.parse(new String[] { "-verbose", "target", "source1", "source2" });
    assertEquals(PrintStreamInfoStream.class, options.config.getInfoStream().getClass());
  }

  public void testMergePolicyOption() throws Exception {
    Options options = Options.parse(new String[] { "-merge-policy", LogDocMergePolicy.class.getName(), "target", "source1", "source2" });
    assertEquals(LogDocMergePolicy.class, options.config.getMergePolicy().getClass());
  }

  public void testMaxSegmentsOption() throws Exception {
    Options options = Options.parse(new String[] { "-max-segments", "42", "target", "source1", "source2" });
    assertEquals(42, options.maxSegments);
  }

}

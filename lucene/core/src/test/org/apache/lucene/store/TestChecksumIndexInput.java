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
package org.apache.lucene.store;

import java.io.IOException;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestChecksumIndexInput extends LuceneTestCase {

  public void testSkipBytes() throws IOException {
    int numTestBytes = TestUtil.nextInt(random(), 100, 1000);
    byte[] testBytes = new byte[numTestBytes];
    final Directory dir = newDirectory();
    IndexOutput os = dir.createOutput("foo", newIOContext(random()));
    os.writeBytes(testBytes, numTestBytes);
    os.close();

    IndexInput is = dir.openInput("foo", newIOContext(random()));
    final InterceptingChecksumIndexInput checksumIndexInput =
        new InterceptingChecksumIndexInput(is, numTestBytes);

    // skip random chunks of bytes until everything has been skipped
    for (int skipped = 0; skipped < numTestBytes; ) {
      final int remaining = numTestBytes - skipped;
      // when remaining gets small enough, just skip the rest
      final int step = remaining < 10 ? remaining : random().nextInt(remaining);
      checksumIndexInput.skipBytes(step);
      skipped += step;
    }

    // ensure all skipped bytes are still "read" in so the checksum can be updated properly
    assertArrayEquals(testBytes, checksumIndexInput.readBytes);

    is.close();
    dir.close();
  }

  /**
   * Captures read bytes into a separate buffer for confirming that all #skipByte invocations
   * delegate to #readBytes.
   */
  private static final class InterceptingChecksumIndexInput extends BufferedChecksumIndexInput {
    final byte[] readBytes;
    private int off = 0;

    public InterceptingChecksumIndexInput(IndexInput main, int len) {
      super(main);
      readBytes = new byte[len];
    }

    @Override
    public void readBytes(final byte[] b, final int offset, final int len) throws IOException {
      super.readBytes(b, offset, len);
      System.arraycopy(b, offset, readBytes, off, len);
      off += len;
    }
  }
}

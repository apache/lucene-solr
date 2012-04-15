package org.apache.lucene.codecs.intblock;

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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.store.*;
import org.apache.lucene.codecs.mockintblock.*;
import org.apache.lucene.codecs.sep.*;

public class TestIntBlockCodec extends LuceneTestCase {

  public void testSimpleIntBlocks() throws Exception {
    Directory dir = newDirectory();

    IntStreamFactory f = new MockFixedIntBlockPostingsFormat(128).getIntFactory();

    IntIndexOutput out = f.createOutput(dir, "test", newIOContext(random()));
    for(int i=0;i<11777;i++) {
      out.write(i);
    }
    out.close();

    IntIndexInput in = f.openInput(dir, "test", newIOContext(random()));
    IntIndexInput.Reader r = in.reader();

    for(int i=0;i<11777;i++) {
      assertEquals(i, r.next());
    }
    in.close();
    
    dir.close();
  }

  public void testEmptySimpleIntBlocks() throws Exception {
    Directory dir = newDirectory();

    IntStreamFactory f = new MockFixedIntBlockPostingsFormat(128).getIntFactory();
    IntIndexOutput out = f.createOutput(dir, "test", newIOContext(random()));

    // write no ints
    out.close();

    IntIndexInput in = f.openInput(dir, "test", newIOContext(random()));
    in.reader();
    // read no ints
    in.close();
    dir.close();
  }
}

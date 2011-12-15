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

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;

public class TestMultiReader extends TestDirectoryReader {

  // TODO: files are never fsynced if you do what this test is doing,
  // so the checkindex is disabled.
  @Override
  protected Directory createDirectory() throws IOException {
    MockDirectoryWrapper mdw = newDirectory();
    mdw.setCheckIndexOnClose(false);
    return mdw;
  }

  @Override
  protected IndexReader openReader() throws IOException {
    IndexReader reader;

    sis.read(dir);
    SegmentReader reader1 = SegmentReader.get(sis.info(0), IndexReader.DEFAULT_TERMS_INDEX_DIVISOR, newIOContext(random));
    SegmentReader reader2 = SegmentReader.get(sis.info(1), IndexReader.DEFAULT_TERMS_INDEX_DIVISOR, newIOContext(random));
    readers[0] = reader1;
    readers[1] = reader2;
    assertTrue(reader1 != null);
    assertTrue(reader2 != null);

    reader = new MultiReader(readers);

    assertTrue(dir != null);
    assertTrue(sis != null);
    
    return reader;
  }

}

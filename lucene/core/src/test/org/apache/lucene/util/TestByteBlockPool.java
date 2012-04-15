package org.apache.lucene.util;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public class TestByteBlockPool extends LuceneTestCase {

  public void testCopyRefAndWrite() throws IOException {
    List<String> list = new ArrayList<String>();
    int maxLength = atLeast(500);
    ByteBlockPool pool = new ByteBlockPool(new ByteBlockPool.DirectAllocator());
    pool.nextBuffer();
    final int numValues = atLeast(100);
    BytesRef ref = new BytesRef();
    for (int i = 0; i < numValues; i++) {
      final String value = _TestUtil.randomRealisticUnicodeString(random(),
          maxLength);
      list.add(value);
      ref.copyChars(value);
      pool.copy(ref);
    }
    RAMDirectory dir = new RAMDirectory();
    IndexOutput stream = dir.createOutput("foo.txt", newIOContext(random()));
    pool.writePool(stream);
    stream.flush();
    stream.close();
    IndexInput input = dir.openInput("foo.txt", newIOContext(random()));
    assertEquals(pool.byteOffset + pool.byteUpto, stream.length());
    BytesRef expected = new BytesRef();
    BytesRef actual = new BytesRef();
    for (String string : list) {
      expected.copyChars(string);
      actual.grow(expected.length);
      actual.length = expected.length;
      input.readBytes(actual.bytes, 0, actual.length);
      assertEquals(expected, actual);
    }
    try {
      input.readByte();
      fail("must be EOF");
    } catch (EOFException e) {
      // expected - read past EOF
    }
    dir.close();
  }
}

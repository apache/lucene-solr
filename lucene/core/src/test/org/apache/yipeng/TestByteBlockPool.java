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

package org.apache.yipeng;

import java.io.IOException;

import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.junit.Assert;

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

public class TestByteBlockPool {
  public static void main(String[] args) throws IOException {
    byteBlockPoolTest();
  }

  public static void byteBlockPoolTest() throws IOException {
    ByteBlockPool byteBlockPool = new ByteBlockPool(new ByteBlockPool.DirectAllocator());

    //要存储的字节ref origin，第一个字节代表长度

    BytesRefBuilder bytesRefBuilder = new BytesRefBuilder();
    for (int i = 0; i <200 ; i++) {
      bytesRefBuilder.append((byte) i);
    }
    BytesRef origin = bytesRefBuilder.get();
    System.out.println(origin);

    //是否要分配新快
    if (origin.length + byteBlockPool.byteUpto > BYTE_BLOCK_SIZE) {
      byteBlockPool.nextBuffer();
    }

    //最终分配资源的起始位置
    int start = byteBlockPool.byteUpto + byteBlockPool.byteOffset;
    byteBlockPool.append(origin);

    //不通过start获取存储的资源（不含长度）
    BytesRef validate = new BytesRef();
    byteBlockPool.setBytesRef(validate, start);

    // skip origin length
    origin.offset += 1;
    origin.length -= 1;

    Assert.assertEquals(origin, validate);
  }
}

package org.apache.lucene.facet.codecs.facet42;

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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;

class Facet42BinaryDocValues extends BinaryDocValues {
  
  private final byte[] bytes;
  private final PackedInts.Reader addresses;
  
  Facet42BinaryDocValues(DataInput in) throws IOException {
    int totBytes = in.readVInt();
    bytes = new byte[totBytes];
    in.readBytes(bytes, 0, totBytes);
    addresses = PackedInts.getReader(in);
  }

  @Override
  public void get(int docID, BytesRef ret) {
    int start = (int) addresses.get(docID);
    ret.bytes = bytes;
    ret.offset = start;
    ret.length = (int) (addresses.get(docID+1)-start);
  }
  
}

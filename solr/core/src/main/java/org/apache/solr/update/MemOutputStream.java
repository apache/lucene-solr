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
package org.apache.solr.update;

import org.apache.solr.common.util.FastOutputStream;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/** @lucene.internal */
public class MemOutputStream extends FastOutputStream {
  public List<byte[]> buffers = new LinkedList<>();
  public MemOutputStream(byte[] tempBuffer) {
    super(null, tempBuffer, 0);
  }

  @Override
  public void flush(byte[] arr, int offset, int len) throws IOException {
    if (arr == buf && offset==0 && len==buf.length) {
      buffers.add(buf);  // steal the buffer
      buf = new byte[8192];
    } else if (len > 0) {
      byte[] newBuf = new byte[len];
      System.arraycopy(arr, offset, newBuf, 0, len);
      buffers.add(newBuf);
    }
  }

  public void writeAll(FastOutputStream fos) throws IOException {
    for (byte[] buffer : buffers) {
      fos.write(buffer);
    }
    if (pos > 0) {
      fos.write(buf, 0, pos);
    }
  }
}

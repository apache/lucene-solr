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

package org.apache.solr.common.util;

public class BytesBlock {
  private int bufSize;
  public byte[] buf;
  //current position
  private int pos;
  //going to expand. mark the start position
  private int startPos = 0;

  public BytesBlock(int sz) {
    this.bufSize = sz;
    create();
  }

  public int getPos() {
    return pos;
  }

  public int getStartPos() {
    return startPos;
  }

  public byte[] getBuf() {
    return buf;
  }

  public BytesBlock expand(int sz) {
    if (bufSize - pos >= sz) {
      return markPositions(sz);
    }
    if (sz > (bufSize / 4)) return new BytesBlock(sz).expand(sz);// a reasonably large block, create new
    create();
    return markPositions(sz);
  }

  private BytesBlock markPositions(int sz) {
    this.startPos = pos;
    pos += sz;
    return this;
  }


  private void create() {
    buf = new byte[bufSize];
    startPos = pos = 0;
  }
}

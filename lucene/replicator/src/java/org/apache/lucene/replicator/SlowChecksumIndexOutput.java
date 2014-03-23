package org.apache.lucene.replicator;

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

import org.apache.lucene.store.IndexOutput;
import java.io.IOException;

/** Silly: sole purpose is to tell SlowChecksumDirectory when it
 *  closed! */
class SlowChecksumIndexOutput extends IndexOutput {
  private final IndexOutput in;
  private final SlowChecksumDirectory dir;
  private final String name;

  public SlowChecksumIndexOutput(SlowChecksumDirectory dir, String name, IndexOutput in) {
    this.in = in;
    this.dir = dir;
    this.name = name;
  }

  @Override
  public void writeByte(byte b) throws IOException {
    in.writeByte(b);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    in.writeBytes(b, offset, length);
  }

  @Override
  public void flush() throws IOException {
    in.flush();
  }

  @Override
  public void close() throws IOException {
    in.close();
    dir.outputClosed(name);
  }

  @Override
  public long getFilePointer() {
    return in.getFilePointer();
  }

  @Override
  public long length() throws IOException {
    return in.length();
  }

  @Override
  public void setLength(long length) throws IOException {
    in.setLength(length);
  }
}



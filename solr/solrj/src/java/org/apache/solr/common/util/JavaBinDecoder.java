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

import java.io.IOException;
import java.io.InputStream;

public class JavaBinDecoder extends JavaBinCodec {
  final FastInputStream dis;

  public JavaBinDecoder(InputStream in) throws IOException {
    this.dis = FastInputStream.wrap(in);
    _init(this.dis);
  }
  public JavaBinDecoder(byte[] buf) throws IOException {
    this(new FastInputStream(null, buf, 0, buf.length));
  }

  public Object readVal() throws IOException {
    return super.readVal(dis);
  }

  public int readInt() throws IOException {
    tagByte = dis.readByte();
    switch (tagByte >>> 5) {

      case SINT >>> 5:
        return readSmallInt(dis);
      case SLONG >>> 5:
        return (int) readSmallLong(dis);
    }

    switch (tagByte) {
      case INT:
        return dis.readInt();
      case LONG:
        return (int) dis.readLong();
      case BYTE:
        return dis.readByte();
      case SHORT:
        return dis.readShort();
      default:throw new IOException("Unexpected type "+ tagByte );
    }
  }

  public long readLong() throws IOException {
    tagByte = dis.readByte();
    switch (tagByte >>> 5) {
      case SINT >>> 5:
        return readSmallInt(dis);
      case SLONG >>> 5:
        return readSmallLong(dis);
    }

    switch (tagByte) {
      case INT:
        return dis.readInt();
      case LONG:
        return dis.readLong();
      case BYTE:
        return dis.readByte();
      case SHORT:
        return dis.readShort();
      default: throw new IOException("Unexpected type "+ tagByte );
    }
  }

  public boolean readBoolean() throws IOException {
    tagByte = dis.readByte();
    switch (tagByte) {
      case BOOL_TRUE:
        return true;
      case BOOL_FALSE:
        return false;
      default: throw new IOException("Unexpected type "+ tagByte );
    }
  }

  public float readFloat() throws IOException {
    tagByte = dis.readByte();
    switch (tagByte) {
      case FLOAT:
        return dis.readFloat();
      case DOUBLE:
        return (float)dis.readDouble();
      default: throw new IOException("Unexpected type "+ tagByte );
    }

  }

  public double readDouble() throws IOException {
    tagByte = dis.readByte();
    switch (tagByte) {
      case FLOAT:
        return dis.readFloat();
      case DOUBLE:
        return dis.readDouble();
      default: throw new IOException("Unexpected type "+ tagByte );
    }

  }
}

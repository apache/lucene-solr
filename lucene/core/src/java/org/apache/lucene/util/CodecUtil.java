package org.apache.lucene.util;

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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

/**
 * Utility class for reading and writing versioned headers.
 * This is useful to ensure that a file is in the format
 * you think it is.
 * @lucene.experimental
 */

public final class CodecUtil {
  private CodecUtil() {} // no instance

  private final static int CODEC_MAGIC = 0x3fd76c17;

  public static void writeHeader(DataOutput out, String codec, int version)
    throws IOException {
    BytesRef bytes = new BytesRef(codec);
    if (bytes.length != codec.length() || bytes.length >= 128) {
      throw new IllegalArgumentException("codec must be simple ASCII, less than 128 characters in length [got " + codec + "]");
    }
    out.writeInt(CODEC_MAGIC);
    out.writeString(codec);
    out.writeInt(version);
  }

  public static int headerLength(String codec) {
    return 9+codec.length();
  }

  public static int checkHeader(DataInput in, String codec, int minVersion, int maxVersion)
    throws IOException {

    // Safety to guard against reading a bogus string:
    final int actualHeader = in.readInt();
    if (actualHeader != CODEC_MAGIC) {
      throw new CorruptIndexException("codec header mismatch: actual header=" + actualHeader + " vs expected header=" + CODEC_MAGIC + " (resource: " + in + ")");
    }

    final String actualCodec = in.readString();
    if (!actualCodec.equals(codec)) {
      throw new CorruptIndexException("codec mismatch: actual codec=" + actualCodec + " vs expected codec=" + codec + " (resource: " + in + ")");
    }

    final int actualVersion = in.readInt();
    if (actualVersion < minVersion) {
      throw new IndexFormatTooOldException(in, actualVersion, minVersion, maxVersion);
    }
    if (actualVersion > maxVersion) {
      throw new IndexFormatTooNewException(in, actualVersion, minVersion, maxVersion);
    }

    return actualVersion;
  }
}

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
package org.apache.lucene.backward_codecs.lucene50;

import java.io.IOException;
import org.apache.lucene.backward_codecs.lucene50.compressing.Lucene50RWCompressingStoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/** RW impersonation of Lucene50StoredFieldsFormat. */
public final class Lucene50RWStoredFieldsFormat extends Lucene50StoredFieldsFormat {

  /** No-argument constructor. */
  public Lucene50RWStoredFieldsFormat() {
    super();
  }

  /** Constructor that takes a mode. */
  public Lucene50RWStoredFieldsFormat(Lucene50StoredFieldsFormat.Mode mode) {
    super(mode);
  }

  @Override
  public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context)
      throws IOException {
    String previous = si.putAttribute(MODE_KEY, mode.name());
    if (previous != null && previous.equals(mode.name()) == false) {
      throw new IllegalStateException(
          "found existing value for "
              + MODE_KEY
              + " for segment: "
              + si.name
              + "old="
              + previous
              + ", new="
              + mode.name());
    }
    return impl(mode).fieldsWriter(directory, si, context);
  }

  @Override
  StoredFieldsFormat impl(Mode mode) {
    switch (mode) {
      case BEST_SPEED:
        return new Lucene50RWCompressingStoredFieldsFormat(
            "Lucene50StoredFieldsFastData", CompressionMode.FAST, 1 << 14, 128, 10);
      case BEST_COMPRESSION:
        return new Lucene50RWCompressingStoredFieldsFormat(
            "Lucene50StoredFieldsHighData", CompressionMode.HIGH_COMPRESSION, 61440, 512, 10);
      default:
        throw new AssertionError();
    }
  }
}

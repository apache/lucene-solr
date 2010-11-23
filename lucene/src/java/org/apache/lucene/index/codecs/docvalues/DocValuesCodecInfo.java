package org.apache.lucene.index.codecs.docvalues;

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
import java.util.Set;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.Reader;
import org.apache.lucene.util.packed.PackedInts.Writer;

/**
 * @lucene.internal
 */
class DocValuesCodecInfo {
  public static final int FORMAT_CURRENT = 0;
  static final String INFO_FILE_EXT = "inf";
  private int[] docValuesFields = new int[1];
  private int max;
  private int pos;

  public DocValuesCodecInfo() {
  }

  void add(int fieldId) {
    if (pos >= docValuesFields.length) {
      docValuesFields = ArrayUtil.grow(docValuesFields, pos + 1);
    }
    docValuesFields[pos++] = fieldId;
    if (fieldId > max) {
      max = fieldId;
    }
  }

  String docValuesId(String segmentsName, String codecID, String fieldId) {
    return segmentsName + "_" + codecID + "-" + fieldId;
  }

  void files(Directory dir, SegmentInfo segmentInfo, String codecId,
      Set<String> files) throws IOException {
    final String file = IndexFileNames.segmentFileName(segmentInfo.name, codecId,
        INFO_FILE_EXT);
    files.add(file);
    for (int i = 0; i < pos; i++) {
      int field = docValuesFields[i];
      String docValuesID = docValuesId(segmentInfo.name, codecId, "" + field);
      files.add(IndexFileNames.segmentFileName(docValuesID, "",
          org.apache.lucene.index.values.Writer.DATA_EXTENSION));
      String idxFile = IndexFileNames.segmentFileName(docValuesID, "",
          org.apache.lucene.index.values.Writer.INDEX_EXTENSION);
      if (dir.fileExists(idxFile)) {
        files.add(idxFile);
      }
    }
  }

  void write(SegmentWriteState state) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(state.segmentName,
        state.codecId, INFO_FILE_EXT);
    final IndexOutput out = state.directory.createOutput(fileName);
    state.flushedFiles.add(fileName);
    try {
      out.writeInt(FORMAT_CURRENT);
      Writer writer = PackedInts.getWriter(out, pos, PackedInts
          .bitsRequired(max));
      for (int i = 0; i < pos; i++) {
        writer.add(docValuesFields[i]);
      }
      writer.finish();
    } finally {
      out.close();
    }

  }

  void read(Directory directory, SegmentInfo info, String codecId)
      throws IOException {
    final String fileName = IndexFileNames.segmentFileName(info.name, codecId,
        INFO_FILE_EXT);
    final IndexInput in = directory.openInput(fileName);
    try {
      in.readInt();
      final Reader reader = PackedInts.getReader(in);
      docValuesFields = new int[reader.size()];
      for (int i = 0; i < docValuesFields.length; i++) {
        docValuesFields[i] = (int) reader.get(i);
      }
      pos = docValuesFields.length;
    } finally {
      in.close();
    }
  }

  IntsRef fieldIDs() {
    return new IntsRef(docValuesFields, 0, pos);
  }
}

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
package org.apache.lucene.codecs.lucene40;

import java.io.IOException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

/**
 * Writer for 4.0 fieldinfos format
 * @deprecated for test purposes only
 */
@Deprecated
public final class Lucene40RWFieldInfosFormat extends Lucene40FieldInfosFormat {

  /** Sole constructor. */
  public Lucene40RWFieldInfosFormat() {
  }
  
  @Override
  public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context) throws IOException {
    if (!segmentSuffix.isEmpty()) {
      throw new UnsupportedOperationException("4.0 does not support fieldinfo updates");
    }
    final String fileName = IndexFileNames.segmentFileName(segmentInfo.name, "", Lucene40FieldInfosFormat.FIELD_INFOS_EXTENSION);
    IndexOutput output = directory.createOutput(fileName, context);
    boolean success = false;
    try {
      CodecUtil.writeHeader(output, Lucene40FieldInfosFormat.CODEC_NAME, Lucene40FieldInfosFormat.FORMAT_CURRENT);
      output.writeVInt(infos.size());
      for (FieldInfo fi : infos) {
        IndexOptions indexOptions = fi.getIndexOptions();
        byte bits = 0x0;
        if (fi.hasVectors()) bits |= Lucene40FieldInfosFormat.STORE_TERMVECTOR;
        if (fi.omitsNorms()) bits |= Lucene40FieldInfosFormat.OMIT_NORMS;
        if (fi.hasPayloads()) bits |= Lucene40FieldInfosFormat.STORE_PAYLOADS;
        if (fi.getIndexOptions() != IndexOptions.NONE) {
          bits |= Lucene40FieldInfosFormat.IS_INDEXED;
          assert indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 || !fi.hasPayloads();
          if (indexOptions == IndexOptions.DOCS) {
            bits |= Lucene40FieldInfosFormat.OMIT_TERM_FREQ_AND_POSITIONS;
          } else if (indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
            bits |= Lucene40FieldInfosFormat.STORE_OFFSETS_IN_POSTINGS;
          } else if (indexOptions == IndexOptions.DOCS_AND_FREQS) {
            bits |= Lucene40FieldInfosFormat.OMIT_POSITIONS;
          }
        }
        output.writeString(fi.name);
        output.writeVInt(fi.number);
        output.writeByte(bits);

        // pack the DV types in one byte
        final byte dv = docValuesByte(fi.getDocValuesType(), fi.getAttribute(LEGACY_DV_TYPE_KEY));
        final byte nrm = docValuesByte(fi.hasNorms() ? DocValuesType.NUMERIC : DocValuesType.NONE, fi.getAttribute(LEGACY_NORM_TYPE_KEY));
        assert (dv & (~0xF)) == 0 && (nrm & (~0x0F)) == 0;
        byte val = (byte) (0xff & ((nrm << 4) | dv));
        output.writeByte(val);
        output.writeStringStringMap(fi.attributes());
      }
      success = true;
    } finally {
      if (success) {
        output.close();
      } else {
        IOUtils.closeWhileHandlingException(output);
      }
    }
  }
  
  /** 4.0-style docvalues byte */
  public byte docValuesByte(DocValuesType type, String legacyTypeAtt) {
    if (type == DocValuesType.NONE) {
      assert legacyTypeAtt == null;
      return 0;
    } else {
      assert legacyTypeAtt != null;
      return (byte) LegacyDocValuesType.valueOf(legacyTypeAtt).ordinal();
    }
  }  
}

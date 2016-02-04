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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene40.Lucene40FieldInfosFormat.LegacyDocValuesType;
import org.apache.lucene.index.BaseFieldInfoFormatTestCase;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;

/** Test Lucene 4.0 FieldInfos Format */
public class TestLucene40FieldInfoFormat extends BaseFieldInfoFormatTestCase {
  private final Codec codec = new Lucene40RWCodec();

  @Override
  protected Codec getCodec() {
    return codec;
  }

  // we only support these three dv types
  @Override
  @Deprecated
  protected DocValuesType[] getDocValuesTypes() {
    return new DocValuesType[] {
      DocValuesType.BINARY,
      DocValuesType.NUMERIC,
      DocValuesType.SORTED
    };
  }

  // but we have more internal typing information, previously recorded in fieldinfos.
  // this is exposed via attributes (so our writer expects them to be set by the dv impl)
  @Override
  protected void addAttributes(FieldInfo fi) {
    DocValuesType dvType = fi.getDocValuesType();
    if (dvType != DocValuesType.NONE) {
      switch (dvType) {
        case BINARY: 
          fi.putAttribute(Lucene40FieldInfosFormat.LEGACY_DV_TYPE_KEY, LegacyDocValuesType.BYTES_FIXED_STRAIGHT.name());
          break;
        case NUMERIC:
          fi.putAttribute(Lucene40FieldInfosFormat.LEGACY_DV_TYPE_KEY, LegacyDocValuesType.FIXED_INTS_32.name());
          break;
        case SORTED:
          fi.putAttribute(Lucene40FieldInfosFormat.LEGACY_DV_TYPE_KEY, LegacyDocValuesType.BYTES_FIXED_SORTED.name());
          break;
        default:
          throw new AssertionError();
      }
    }
    
    if (fi.hasNorms()) {
      fi.putAttribute(Lucene40FieldInfosFormat.LEGACY_NORM_TYPE_KEY, LegacyDocValuesType.FIXED_INTS_8.name());
    }
  }
}

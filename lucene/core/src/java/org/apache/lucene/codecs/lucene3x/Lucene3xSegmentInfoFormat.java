package org.apache.lucene.codecs.lucene3x;

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

import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.SegmentInfoReader;
import org.apache.lucene.codecs.SegmentInfoWriter;
import org.apache.lucene.index.SegmentInfo;

/**
 * Lucene3x ReadOnly SegmentInfoFormat implementation
 * @deprecated (4.0) This is only used to read indexes created
 * before 4.0.
 * @lucene.experimental
 */
@Deprecated
public class Lucene3xSegmentInfoFormat extends SegmentInfoFormat {
  private final SegmentInfoReader reader = new Lucene3xSegmentInfoReader();

  /** This format adds optional per-segment String
   *  diagnostics storage, and switches userData to Map */
  public static final int FORMAT_DIAGNOSTICS = -9;

  /** Each segment records whether it has term vectors */
  public static final int FORMAT_HAS_VECTORS = -10;

  /** Each segment records the Lucene version that created it. */
  public static final int FORMAT_3_1 = -11;

  /** Extension used for saving each SegmentInfo, once a 3.x
   *  index is first committed to with 4.0. */
  public static final String UPGRADED_SI_EXTENSION = "si";
  public static final String UPGRADED_SI_CODEC_NAME = "Lucene3xSegmentInfo";
  public static final int UPGRADED_SI_VERSION_START = 0;
  public static final int UPGRADED_SI_VERSION_CURRENT = UPGRADED_SI_VERSION_START;
  
  @Override
  public SegmentInfoReader getSegmentInfoReader() {
    return reader;
  }

  @Override
  public SegmentInfoWriter getSegmentInfoWriter() {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }
  
  // only for backwards compat
  public static final String DS_OFFSET_KEY = Lucene3xSegmentInfoFormat.class.getSimpleName() + ".dsoffset";
  public static final String DS_NAME_KEY = Lucene3xSegmentInfoFormat.class.getSimpleName() + ".dsname";
  public static final String DS_COMPOUND_KEY = Lucene3xSegmentInfoFormat.class.getSimpleName() + ".dscompound";
  public static final String NORMGEN_KEY = Lucene3xSegmentInfoFormat.class.getSimpleName() + ".normgen";
  public static final String NORMGEN_PREFIX = Lucene3xSegmentInfoFormat.class.getSimpleName() + ".normfield";

  /** 
   * @return if this segment shares stored fields & vectors, this
   *         offset is where in that file this segment's docs begin 
   */
  public static int getDocStoreOffset(SegmentInfo si) {
    String v = si.getAttribute(DS_OFFSET_KEY);
    return v == null ? -1 : Integer.parseInt(v);
  }
  
  /** @return name used to derive fields/vectors file we share with other segments */
  public static String getDocStoreSegment(SegmentInfo si) {
    String v = si.getAttribute(DS_NAME_KEY);
    return v == null ? si.name : v;
  }
  
  /** @return whether doc store files are stored in compound file (*.cfx) */
  public static boolean getDocStoreIsCompoundFile(SegmentInfo si) {
    String v = si.getAttribute(DS_COMPOUND_KEY);
    return v == null ? false : Boolean.parseBoolean(v);
  }
}

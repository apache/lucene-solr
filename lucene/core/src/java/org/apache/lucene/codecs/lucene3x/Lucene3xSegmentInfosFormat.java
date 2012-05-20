package org.apache.lucene.codecs.lucene3x;

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

import java.util.Set;

import org.apache.lucene.codecs.SegmentInfosFormat;
import org.apache.lucene.codecs.SegmentInfosReader;
import org.apache.lucene.codecs.SegmentInfosWriter;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;

/**
 * Lucene3x ReadOnly SegmentInfosFormat implementation
 * @deprecated (4.0) This is only used to read indexes created
 * before 4.0.
 * @lucene.experimental
 */
@Deprecated
public class Lucene3xSegmentInfosFormat extends SegmentInfosFormat {
  private final SegmentInfosReader reader = new Lucene3xSegmentInfosReader();

  // nocommit explain or remove this!:
  public static final String SI_EXTENSION = "si";
  
  /** This format adds optional per-segment String
   *  diagnostics storage, and switches userData to Map */
  public static final int FORMAT_DIAGNOSTICS = -9;

  /** Each segment records whether it has term vectors */
  public static final int FORMAT_HAS_VECTORS = -10;

  /** Each segment records the Lucene version that created it. */
  public static final int FORMAT_3_1 = -11;

  // nocommit we should nuke FORMAT_4_0!?

  /** Each segment records whether its postings are written
   *  in the new flex format */
  public static final int FORMAT_4_0 = -12;

  /** This must always point to the most recent file format.
   * whenever you add a new format, make it 1 smaller (negative version logic)! */
  // TODO: move this, as its currently part of required preamble
  public static final int FORMAT_CURRENT = FORMAT_4_0;
  
  /** This must always point to the first supported file format. */
  public static final int FORMAT_MINIMUM = FORMAT_DIAGNOSTICS;

  @Override
  public SegmentInfosReader getSegmentInfosReader() {
    return reader;
  }

  @Override
  public SegmentInfosWriter getSegmentInfosWriter() {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }

  @Override
  public void files(SegmentInfo info, Set<String> files) {
    // nocommit must take care to filter this out if we are
    // "really" an old 3.x index
    files.add(IndexFileNames.segmentFileName(info.name, "", SI_EXTENSION));
  }
}

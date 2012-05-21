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

  /** This format adds optional per-segment String
   *  diagnostics storage, and switches userData to Map */
  public static final int FORMAT_DIAGNOSTICS = -9;

  /** Each segment records whether it has term vectors */
  public static final int FORMAT_HAS_VECTORS = -10;

  /** Each segment records the Lucene version that created it. */
  public static final int FORMAT_3_1 = -11;

  /** Each segment records whether its postings are written
   *  in the new flex format */
  public static final int FORMAT_4X_UPGRADE = -12;

  /** Extension used for saving each SegmentInfo, once a 3.x
   *  index is first committed to with 4.0. */
  public static final String SI_EXTENSION = "si";
  
  @Override
  public SegmentInfosReader getSegmentInfosReader() {
    return reader;
  }

  @Override
  public SegmentInfosWriter getSegmentInfosWriter() {
    throw new UnsupportedOperationException("this codec can only be used for reading");
  }
}

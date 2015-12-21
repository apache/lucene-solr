package org.apache.lucene.codecs.simpletext;

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

import java.io.IOException;

import org.apache.lucene.codecs.DimensionalFormat;
import org.apache.lucene.codecs.DimensionalReader;
import org.apache.lucene.codecs.DimensionalWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/** For debugging, curiosity, transparency only!!  Do not
 *  use this codec in production.
 *
 *  <p>This codec stores all dimensional data in a single
 *  human-readable text file (_N.dim).  You can view this in
 *  any text editor, and even edit it to alter your index.
 *
 *  @lucene.experimental */
public final class SimpleTextDimensionalFormat extends DimensionalFormat {
  
  @Override
  public DimensionalWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new SimpleTextDimensionalWriter(state);
  }

  @Override
  public DimensionalReader fieldsReader(SegmentReadState state) throws IOException {
    return new SimpleTextDimensionalReader(state);
  }

  /** Extension of dimensional data file */
  static final String DIMENSIONAL_EXTENSION = "dim";

  /** Extension of dimensional index file */
  static final String DIMENSIONAL_INDEX_EXTENSION = "dii";
}

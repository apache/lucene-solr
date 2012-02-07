package org.apache.lucene.codecs;

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

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * Specifies an API for classes that can read {@link SegmentInfos} information.
 * @lucene.experimental
 */
public abstract class SegmentInfosReader {

  /**
   * Read {@link SegmentInfos} data from a directory.
   * @param directory directory to read from
   * @param segmentsFileName name of the "segments_N" file
   * @param header input of "segments_N" file after reading preamble
   * @param infos empty instance to be populated with data
   * @throws IOException
   */
  public abstract void read(Directory directory, String segmentsFileName, ChecksumIndexInput header, SegmentInfos infos, IOContext context) throws IOException;
}

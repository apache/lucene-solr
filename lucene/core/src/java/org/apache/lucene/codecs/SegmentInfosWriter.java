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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

/**
 * Specifies an API for classes that can write out {@link SegmentInfos} data.
 * @lucene.experimental
 */
public abstract class SegmentInfosWriter {

  /**
   * Write {@link SegmentInfos} data without closing the output. The returned
   * output will become finished only after a successful completion of
   * "two phase commit" that first calls {@link #prepareCommit(IndexOutput)} and
   * then {@link #finishCommit(IndexOutput)}.
   * @param dir directory to write data to
   * @param segmentsFileName name of the "segments_N" file to create
   * @param infos data to write
   * @return an instance of {@link IndexOutput} to be used in subsequent "two
   * phase commit" operations as described above.
   * @throws IOException
   */
  public abstract IndexOutput writeInfos(Directory dir, String segmentsFileName, String codecID, SegmentInfos infos, IOContext context) throws IOException;
  
  /**
   * First phase of the two-phase commit - ensure that all output can be
   * successfully written out.
   * @param out an instance of {@link IndexOutput} returned from a previous
   * call to {@link #writeInfos(Directory, String, String, SegmentInfos, IOContext)}.
   * @throws IOException
   */
  public abstract void prepareCommit(IndexOutput out) throws IOException;
  
  /**
   * Second phase of the two-phase commit. In this step the output should be
   * finalized and closed.
   * @param out an instance of {@link IndexOutput} returned from a previous
   * call to {@link #writeInfos(Directory, String, String, SegmentInfos, IOContext)}.
   * @throws IOException
   */
  public abstract void finishCommit(IndexOutput out) throws IOException;
}

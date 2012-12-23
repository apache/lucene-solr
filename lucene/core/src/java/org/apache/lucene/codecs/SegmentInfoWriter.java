package org.apache.lucene.codecs;

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

import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * Specifies an API for classes that can write out {@link SegmentInfo} data.
 * @lucene.experimental
 */

public abstract class SegmentInfoWriter {
  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected SegmentInfoWriter() {
  }

  /**
   * Write {@link SegmentInfo} data. 

   * @param dir directory to write to
   * @param info the segment info to write
   * @param fis field infos to use
   * @param ioContext IO context to use
   * @throws IOException If an I/O error occurs
   */
  public abstract void write(Directory dir, SegmentInfo info, FieldInfos fis, IOContext ioContext) throws IOException;
  
  /**
   * Write the list of files belonging to an updates segment of the segment with
   * {@link SegmentInfo}, with the given updates generation.
   * 
   * @param dir directory to write to
   * @param info info of the segment to write
   * @param generation updates generation, or 0 for segment base
   * @param ioContext IO context to use
   * @throws IOException
   *           If an I/O error occurs
   */
  public abstract void writeFilesList(Directory dir, SegmentInfo info, long generation, IOContext ioContext) throws IOException;
}

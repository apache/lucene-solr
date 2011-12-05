package org.apache.lucene.index.codecs;

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

import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * format for normalization factors
 */
public abstract class NormsFormat {
  /** Note: separateNormsDir should not be used! */
  public abstract NormsReader normsReader(Directory dir, SegmentInfo info, FieldInfos fields, IOContext context, Directory separateNormsDir) throws IOException;
  public abstract NormsWriter normsWriter(SegmentWriteState state) throws IOException;
  public abstract void files(Directory dir, SegmentInfo info, Set<String> files) throws IOException;
  
  /** 
   * Note: this should not be overridden! 
   * @deprecated 
   */
  @Deprecated
  public void separateFiles(Directory dir, SegmentInfo info, Set<String> files) throws IOException {};
}

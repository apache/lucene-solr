package org.apache.lucene.index;

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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * @lucene.experimental
 */
public class SegmentReadState {
  public final Directory dir;
  public final SegmentInfo segmentInfo;
  public final FieldInfos fieldInfos;
  public final IOContext context;

  // NOTE: if this is < 0, that means "defer terms index
  // load until needed".  But if the codec must load the
  // terms index on init (preflex is the only once currently
  // that must do so), then it should negate this value to
  // get the app's terms divisor:
  public int termsIndexDivisor;
  public final String segmentSuffix;

  public SegmentReadState(Directory dir, SegmentInfo info,
      FieldInfos fieldInfos, IOContext context, int termsIndexDivisor) {
    this(dir, info, fieldInfos,  context, termsIndexDivisor, "");
  }
  
  public SegmentReadState(Directory dir,
                          SegmentInfo info,
                          FieldInfos fieldInfos,
                          IOContext context,
                          int termsIndexDivisor,
                          String segmentSuffix) {
    this.dir = dir;
    this.segmentInfo = info;
    this.fieldInfos = fieldInfos;
    this.context = context;
    this.termsIndexDivisor = termsIndexDivisor;
    this.segmentSuffix = segmentSuffix;
  }

  public SegmentReadState(SegmentReadState other,
                          String newSegmentSuffix) {
    this.dir = other.dir;
    this.segmentInfo = other.segmentInfo;
    this.fieldInfos = other.fieldInfos;
    this.context = other.context;
    this.termsIndexDivisor = other.termsIndexDivisor;
    this.segmentSuffix = newSegmentSuffix;
  }
}

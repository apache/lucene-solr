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

/**
 * Expert: Controls the format of the segments file.
 * Note, this isn't a per-segment file, if you change the format, other versions
 * of lucene won't be able to read it, yackedy schmackedy
 * 
 * @lucene.experimental
 */
// TODO: would be great to handle this situation better.
// ideally a custom implementation could implement two-phase commit differently,
// (e.g. atomic rename), and ideally all versions of lucene could still read it.
// but this is just reflecting reality as it is today...
//
// also, perhaps the name should change (to cover all global files like .fnx?)
// then again, maybe we can just remove that file...
public abstract class SegmentInfosFormat {
  public abstract SegmentInfosReader getSegmentInfosReader();
  public abstract SegmentInfosWriter getSegmentInfosWriter();
}

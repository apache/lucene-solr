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
package org.apache.lucene.codecs.lucene62;

import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.lucene53.Lucene53RWNormsFormat;
import org.apache.lucene.codecs.lucene62.Lucene62Codec;

/**
 * Read-write version of 6.2 codec for testing
 * @deprecated for test purposes only
 */
@Deprecated
public class Lucene62RWCodec extends Lucene62Codec {

  private final SegmentInfoFormat segmentInfoFormat = new Lucene62RWSegmentInfoFormat();
  private final NormsFormat normsFormat = new Lucene53RWNormsFormat();

  @Override
  public SegmentInfoFormat segmentInfoFormat() {
    return segmentInfoFormat;
  }
  
  @Override
  public NormsFormat normsFormat() {
    return normsFormat;
  }

}

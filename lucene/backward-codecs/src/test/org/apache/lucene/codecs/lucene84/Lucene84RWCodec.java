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
package org.apache.lucene.codecs.lucene84;

import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50RWStoredFieldsFormat;
import org.apache.lucene.codecs.lucene60.Lucene60RWPointsFormat;
import org.apache.lucene.codecs.lucene70.Lucene70RWSegmentInfoFormat;

/**
 * RW impersonation of {@link Lucene84Codec}.
 */
public class Lucene84RWCodec extends Lucene84Codec {

  @Override
  public PointsFormat pointsFormat() {
    return new Lucene60RWPointsFormat();
  }

  @Override
  public SegmentInfoFormat segmentInfoFormat() {
    return new Lucene70RWSegmentInfoFormat();
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return new Lucene50RWStoredFieldsFormat();
  }

}

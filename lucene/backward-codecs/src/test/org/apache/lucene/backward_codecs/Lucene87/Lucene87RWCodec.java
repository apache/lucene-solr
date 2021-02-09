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
package org.apache.lucene.backward_codecs.Lucene87;

import org.apache.lucene.backward_codecs.lucene50.Lucene50RWCompoundFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50RWTermVectorsFormat;
import org.apache.lucene.backward_codecs.lucene87.Lucene87Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.TermVectorsFormat;

/** RW impersonation of {@link Lucene87Codec}. */
public class Lucene87RWCodec extends Lucene87Codec {

  @Override
  public final CompoundFormat compoundFormat() {
    return new Lucene50RWCompoundFormat();
  }

  @Override
  public TermVectorsFormat termVectorsFormat() {
    return new Lucene50RWTermVectorsFormat();
  }
}

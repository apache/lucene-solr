package org.apache.lucene.index.codecs.spi;

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

import java.util.Set;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.PostingsFormat;

/**
 * Common base class for CodecProviders that uses maps to implement lookup
 * @lucene.internal
 */
public abstract class SimpleCodecProvider implements CodecProvider {

  private final Map<String,Codec> availableCodecs = new LinkedHashMap<String,Codec>();
  private final Map<String,PostingsFormat> availablePostingsFormats = new LinkedHashMap<String,PostingsFormat>();

  protected SimpleCodecProvider(List<Codec> codecs, List<PostingsFormat> postingsFormats) {
    for (Codec codec : codecs) {
      availableCodecs.put(codec.getName(), codec);
    }
    for (PostingsFormat pf : postingsFormats) {
      availablePostingsFormats.put(pf.name, pf);
    }
  }

  @Override
  public final Codec lookupCodec(String name) {
    return availableCodecs.get(name);
  }

  @Override
  public final Set<String> availableCodecs() {
    return Collections.unmodifiableSet(availableCodecs.keySet());
  }

  @Override
  public final PostingsFormat lookupPostingsFormat(String name) {
    return availablePostingsFormats.get(name);
  }

  @Override
  public final Set<String> availablePostingsFormats() {
    return Collections.unmodifiableSet(availablePostingsFormats.keySet());
  }

}

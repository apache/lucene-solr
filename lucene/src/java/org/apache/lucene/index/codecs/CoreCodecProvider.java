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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.codecs.lucene3x.Lucene3xCodec;
import org.apache.lucene.index.codecs.lucene40.Lucene40Codec;

/**
 * A CodecProvider that registers all core codecs that ship
 * with Lucene.  This will not register any user codecs, but
 * you can easily instantiate this class and register them
 * yourself and specify the default codec for new segments:
 * nocommit: fix docs here
 * <pre>
 *   CodecProvider cp = new CoreCodecProvider();
 *   cp.register(new MyFastCodec());
 *   cp.setDefaultCodec("MyFastCodec");
 *   IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
 *   iwc.setCodecProvider(cp);
 * </pre>
 */

public class CoreCodecProvider extends CodecProvider {
  @Override
  public Codec getDefaultCodec() {
    return lookup("Lucene40");
  }

  @Override
  public Codec lookup(String name) {
    return CORE_CODECS.get(name);
  }

  // nocommit should we make this an unmodifiable map?
  /** Lucene's core codecs
   *  @lucene.internal
   */
  public static final Map<String,Codec> CORE_CODECS = new HashMap<String,Codec>();
  static {
    CORE_CODECS.put("Lucene40", new Lucene40Codec());
    CORE_CODECS.put("Lucene3x", new Lucene3xCodec());
  }
}

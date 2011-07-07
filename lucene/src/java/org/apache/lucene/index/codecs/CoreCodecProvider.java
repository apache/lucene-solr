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

import org.apache.lucene.index.codecs.memory.MemoryCodec;
import org.apache.lucene.index.codecs.preflex.PreFlexCodec;
import org.apache.lucene.index.codecs.pulsing.PulsingCodec;
import org.apache.lucene.index.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.index.codecs.standard.StandardCodec;

/**
 * A CodecProvider that registers all core codecs that ship
 * with Lucene.  This will not register any user codecs, but
 * you can easily instantiate this class and register them
 * yourself and specify per-field codecs:
 * 
 * <pre>
 *   CodecProvider cp = new CoreCodecProvider();
 *   cp.register(new MyFastCodec());
 *   cp.setDefaultFieldCodec("Standard");
 *   cp.setFieldCodec("id", "Pulsing");
 *   cp.setFieldCodec("body", "MyFastCodec");
 *   IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
 *   iwc.setCodecProvider(cp);
 * </pre>
 */

public class CoreCodecProvider extends CodecProvider {
  public CoreCodecProvider() {
    register(new StandardCodec());
    register(new PreFlexCodec());
    register(new PulsingCodec());
    register(new SimpleTextCodec());
    register(new MemoryCodec());
  }
}

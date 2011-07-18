package org.apache.solr.core;

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

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.pulsing.PulsingCodec;
import org.apache.lucene.index.codecs.standard.StandardCodec;
import org.apache.solr.common.util.NamedList;

/**
 * CodecProviderFactory for testing, it inits a CP with Standard and Pulsing,
 * and also adds any codecs specified by classname in solrconfig.
 */
public class MockCodecProviderFactory extends CodecProviderFactory {
  private String defaultCodec;
  private NamedList codecs;

  @Override
  public void init(NamedList args) {
    super.init(args);
    defaultCodec = (String) args.get("defaultCodec");
    codecs = (NamedList) args.get("codecs");
  }

  @Override
  public CodecProvider create() {
    CodecProvider cp = new CodecProvider();
    cp.register(new StandardCodec());
    cp.register(new PulsingCodec());
    if (codecs != null) {
      for (Object codec : codecs.getAll("name")) {
        if (!cp.isCodecRegistered((String)codec)) {
          try {
            Class<? extends Codec> clazz = Class.forName((String)codec).asSubclass(Codec.class);
            cp.register(clazz.newInstance());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
    if (defaultCodec != null) {
      cp.setDefaultFieldCodec(defaultCodec);
    }
    return cp;
  }
}

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

import java.util.Collections;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.ServiceLoader;

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.PostingsFormat;

/**
 * Helper class for loading {@link Codec} and {@link PostingsFormat} using SPI.
 * TODO: Somehow remove synchronization, ServiceLoader is not threadsafe?
 * @lucene.internal
 */
public final class CodecLoader {

  private static final ServiceLoader<CodecProvider> loader =
    ServiceLoader.load(CodecProvider.class);
  private static Set<String> codecCache = null, pfCache = null;

  private CodecLoader() {}
  
  public static synchronized void reload() {
    loader.reload();
    codecCache = null;
    pfCache = null;
  }

  public static synchronized Codec lookupCodec(String name) {
    for (CodecProvider cp : loader) {
      final Codec codec = cp.lookupCodec(name);
      if (codec != null) return codec;
    }
    throw new RuntimeException("A codec with name '"+name+"' does not exist. "+
     "You need to add the corresponding JAR file with a CodecProvider supporting this codec to your classpath.");
  }

  public static synchronized Set<String> availableCodecs() {
    if (codecCache == null) {
      final Set<String> s = new LinkedHashSet<String>();
      for (CodecProvider cp : loader) {
        s.addAll(cp.availableCodecs());
      }
      codecCache = Collections.unmodifiableSet(s);
    }
    return codecCache;
  }

  public static synchronized PostingsFormat lookupPostingsFormat(String name) {
    for (CodecProvider cp : loader) {
      final PostingsFormat pf = cp.lookupPostingsFormat(name);
      if (pf != null) return pf;
    }
    throw new RuntimeException("A postings format with name '"+name+"' does not exist. "+
     "You need to add the corresponding JAR file with a CodecProvider supporting this format to your classpath.");
  }

  public static synchronized Set<String> availablePostingsFormats() {
    if (pfCache == null) {
      final Set<String> s = new LinkedHashSet<String>();
      for (CodecProvider cp : loader) {
        s.addAll(cp.availablePostingsFormats());
      }
      pfCache = Collections.unmodifiableSet(s);
    }
    return pfCache;
  }
  
}

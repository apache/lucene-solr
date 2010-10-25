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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.codecs.preflex.PreFlexCodec;
import org.apache.lucene.index.codecs.pulsing.PulsingCodec;
import org.apache.lucene.index.codecs.standard.StandardCodec;
import org.apache.lucene.index.codecs.simpletext.SimpleTextCodec;

/** Holds a set of codecs, keyed by name.  You subclass
 *  this, instantiate it, and register your codecs, then
 *  pass this instance to IndexReader/IndexWriter (via
 *  package private APIs) to use different codecs when
 *  reading & writing segments. 
 *
 *  @lucene.experimental */

public abstract class CodecProvider {
  private SegmentInfosWriter infosWriter = new DefaultSegmentInfosWriter();
  private SegmentInfosReader infosReader = new DefaultSegmentInfosReader();

  private final HashMap<String, Codec> codecs = new HashMap<String, Codec>();

  private final Set<String> knownExtensions = new HashSet<String>();

  private static String defaultCodec = "Standard";

  public final static String[] CORE_CODECS = new String[] {"Standard", "Pulsing", "PreFlex", "SimpleText"};

  public synchronized void register(Codec codec) {
    if (codec.name == null) {
      throw new IllegalArgumentException("code.name is null");
    }
    if (!codecs.containsKey(codec.name)) {
      codecs.put(codec.name, codec);
      codec.getExtensions(knownExtensions);
    } else if (codecs.get(codec.name) != codec) {
      throw new IllegalArgumentException("codec '" + codec.name + "' is already registered as a different codec instance");
    }
  }
  
  /** @lucene.internal */
  public synchronized void unregister(Codec codec) {
    if (codec.name == null) {
      throw new IllegalArgumentException("code.name is null");
    }
    if (codecs.containsKey(codec.name)) {
      Codec c = codecs.get(codec.name);
      if (codec == c) {
        codecs.remove(codec.name);
      } else {
        throw new IllegalArgumentException("codec '" + codec.name + "' is being impersonated by a different codec instance!!!");
      }
    }
  }

  public Collection<String> getAllExtensions() {
    return knownExtensions;
  }

  public synchronized Codec lookup(String name) {
    final Codec codec = codecs.get(name);
    if (codec == null)
      throw new IllegalArgumentException("required codec '" + name + "' not found");
    return codec;
  }

  public abstract Codec getWriter(SegmentWriteState state);
  
  public SegmentInfosWriter getSegmentInfosWriter() {
    return infosWriter;
  }
  
  public SegmentInfosReader getSegmentInfosReader() {
    return infosReader;
  }

  static private final CodecProvider defaultCodecs = new DefaultCodecProvider();

  public static CodecProvider getDefault() {
    return defaultCodecs;
  }

  /** Used for testing. @lucene.internal */
  public synchronized static void setDefaultCodec(String s) {
    defaultCodec = s;
  }
  /** Used for testing. @lucene.internal */
  public synchronized static String getDefaultCodec() {
    return defaultCodec;
  }
}

class DefaultCodecProvider extends CodecProvider {
  DefaultCodecProvider() {
    register(new StandardCodec());
    register(new PreFlexCodec());
    register(new PulsingCodec(1));
    register(new SimpleTextCodec());
  }

  @Override
  public Codec getWriter(SegmentWriteState state) {
    return lookup(CodecProvider.getDefaultCodec());
  }
}

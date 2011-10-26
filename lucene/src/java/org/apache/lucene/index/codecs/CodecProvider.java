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

/** Holds a set of codecs, keyed by name.  You subclass
 *  this, instantiate it, and register your codecs, then
 *  pass this instance to IndexReader/IndexWriter (via
 *  package private APIs) to use different codecs when
 *  reading & writing segments. 
 *
 *  @lucene.experimental */

public class CodecProvider {
  private SegmentInfosWriter infosWriter = new DefaultSegmentInfosWriter();
  private SegmentInfosReader infosReader = new DefaultSegmentInfosReader();

  private final HashMap<String, Codec> codecs = new HashMap<String, Codec>();

  private final Set<String> knownExtensions = new HashSet<String>();


  public final static String[] CORE_CODECS = new String[] { "PerField", "PreFlex" };

  public synchronized void register(Codec codec) {
    if (codec.getName() == null) {
      throw new IllegalArgumentException("codec.getName() is null");
    }
    if (!codecs.containsKey(codec.getName())) {
      codecs.put(codec.getName(), codec);
      codec.getExtensions(knownExtensions);
    } else if (codecs.get(codec.getName()) != codec) {
      throw new IllegalArgumentException("codec '" + codec.getName() + "' is already registered as a different codec instance");
    }
  }
  
  /** @lucene.internal */
  public synchronized void unregister(Codec codec) {
    if (codec.getName() == null) {
      throw new IllegalArgumentException("code.name is null");
    }
    if (codecs.containsKey(codec.getName())) {
      Codec c = codecs.get(codec.getName());
      if (codec == c) {
        codecs.remove(codec.getName());
      } else {
        throw new IllegalArgumentException("codec '" + codec.getName() + "' is being impersonated by a different codec instance!!!");
      }
    }
  }
  
  /** @lucene.internal */
  public synchronized Set<String> listAll() {
    return codecs.keySet();
  }

  public Collection<String> getAllExtensions() {
    return knownExtensions;
  }

  public synchronized Codec lookup(String name) {
    final Codec codec = codecs.get(name);
    if (codec == null) {
      throw new IllegalArgumentException("required codec '" + name + "' not found; known codecs: " + codecs.keySet());
    }
    return codec;
  }

  /**
   * Returns <code>true</code> iff a codec with the given name is registered
   * @param name codec name
   * @return <code>true</code> iff a codec with the given name is registered, otherwise <code>false</code>.
   */
  public synchronized boolean isCodecRegistered(String name) {
    return codecs.containsKey(name);
  }

  public SegmentInfosWriter getSegmentInfosWriter() {
    return infosWriter;
  }
  
  public SegmentInfosReader getSegmentInfosReader() {
    return infosReader;
  }
  
  static private CodecProvider defaultCodecs = new CoreCodecProvider();

  public static CodecProvider getDefault() {
    return defaultCodecs;
  }

  /** For testing only
   *  @lucene.internal */
  public static void setDefault(CodecProvider cp) {
    defaultCodecs = cp;
  }
}

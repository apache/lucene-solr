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
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

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
  private String defaultFieldCodec = "Standard";
  private final Map<String, String> perFieldMap = new HashMap<String, String>();

  
  private final HashMap<String, Codec> codecs = new HashMap<String, Codec>();

  private final Set<String> knownExtensions = new HashSet<String>();


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
  
  /** @lucene.internal */
  public synchronized Set<String> listAll() {
    return codecs.keySet();
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
  
  /**
   * Sets the {@link Codec} for a given field. Not that setting a field's codec is
   * write-once. If the field's codec is already set this method will throw an
   * {@link IllegalArgumentException}.
   * 
   * @param field
   *          the name of the field
   * @param codec
   *          the name of the codec
   * @throws IllegalArgumentException
   *           if the codec for the given field is already set
   * 
   */
  public synchronized void setFieldCodec(String field, String codec) {
    if (perFieldMap.containsKey(field))
      throw new IllegalArgumentException("codec for field: " + field
          + " already set to " + perFieldMap.get(field));
    perFieldMap.put(field, codec);
  }

  /**
   * Returns the {@link Codec} name for the given field or the default codec if
   * not set.
   * 
   * @param name
   *          the fields name
   * @return the {@link Codec} name for the given field or the default codec if
   *         not set.
   */
  public synchronized String getFieldCodec(String name) {
    final String codec;
    if ((codec = perFieldMap.get(name)) == null) {
      return defaultFieldCodec;
    }
    return codec;
  }

  /**
   * Returns <code>true</code> if this provider has a Codec registered for this
   * field.
   */
  public synchronized boolean hasFieldCodec(String name) {
    return perFieldMap.containsKey(name);
  }

  /**
   * Returns the default {@link Codec} for this {@link CodecProvider}
   * 
   * @return the default {@link Codec} for this {@link CodecProvider}
   */
  public synchronized String getDefaultFieldCodec() {
    return defaultFieldCodec;
  }

  /**
   * Sets the default {@link Codec} for this {@link CodecProvider}
   * 
   * @param codec
   *          the codecs name
   */
  public synchronized void setDefaultFieldCodec(String codec) {
    defaultFieldCodec = codec;
  }
  
  /**
   * Registers all codecs from the given provider including the field to codec
   * mapping and the default field codec.
   * <p>
   * NOTE: This method will pass any codec from the given codec to
   * {@link #register(Codec)} and sets fiels codecs via
   * {@link #setFieldCodec(String, String)}.
   */
  public void copyFrom(CodecProvider other) {
    final Collection<Codec> values = other.codecs.values();
    for (Codec codec : values) {
      register(codec);
    }
    final Set<Entry<String, String>> entrySet = other.perFieldMap.entrySet();
    for (Entry<String, String> entry : entrySet) {
      setFieldCodec(entry.getKey(), entry.getValue());
    }
    setDefaultFieldCodec(other.getDefaultFieldCodec());
  }
}

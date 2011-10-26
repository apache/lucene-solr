package org.apache.lucene.index.codecs.perfield;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.DefaultFieldsFormat;
import org.apache.lucene.index.codecs.FieldsFormat;
import org.apache.lucene.index.codecs.PostingsFormat;
import org.apache.lucene.index.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.index.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.index.codecs.pulsing.PulsingPostingsFormat;
import org.apache.lucene.index.codecs.simpletext.SimpleTextPostingsFormat;

// nocommit: clean all this up, and just make it PerFieldPostingsFormatWrapper
// also we can't have an immutable per-field map given how solr uses it.

public abstract class PerFieldCodec extends Codec {
  private final PostingsFormat postingsFormat;

  private final String defaultPostingsFormat;
  private final Map<String,String> perFieldMap;
  
  public PerFieldCodec(String name, String defaultPostingsFormat, Map<String,String> perFieldMap) {
    super(name);
    this.defaultPostingsFormat = defaultPostingsFormat;
    this.perFieldMap = perFieldMap;
    this.postingsFormat = new PerFieldPostingsFormat(this);
  }

  @Override
  public PostingsFormat postingsFormat() {
    return postingsFormat;
  }
  
  /**
   * Returns the {@link PostingsFormat} name for the given field or the default format if
   * not set.
   * 
   * @param name
   *          the fields name
   * @return the {@link PostingsFormat} name for the given field or the default format if
   *         not set.
   */
  public String getPostingsFormat(String name) {
    final String format;
    if ((format = perFieldMap.get(name)) == null) {
      return defaultPostingsFormat;
    }
    return format;
  }

  /**
   * Returns <code>true</code> if this Codec has a Format registered for this
   * field.
   */
  public boolean hasPostingsFormatFormat(String name) {
    return perFieldMap.containsKey(name);
  }

  /**
   * Returns the default {@link PostingsFormat} for this {@link Codec}
   * 
   * @return the default {@link PostingsFormat} for this {@link Codec}
   */
  public String getDefaultPostingsFormat() {
    return defaultPostingsFormat;
  }
  
  public abstract PostingsFormat lookup(String name);
}

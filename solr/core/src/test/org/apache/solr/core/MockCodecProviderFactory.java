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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.CoreCodecProvider;
import org.apache.lucene.index.codecs.PostingsFormat;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.lucene40.Lucene40Codec;
import org.apache.lucene.index.codecs.lucene40.Lucene40PostingsBaseFormat;
import org.apache.lucene.index.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.index.codecs.pulsing.PulsingPostingsFormat;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

/**
 * CodecProviderFactory for testing, it inits a CP with Standard and Pulsing,
 * and also adds any codecs specified by classname in solrconfig.
 */
public class MockCodecProviderFactory extends CodecProviderFactory {
  private String defaultFormat;
  private NamedList formats;

  @Override
  public void init(NamedList args) {
    super.init(args);
    defaultFormat = (String) args.get("defaultPostingsFormat");
    formats = (NamedList) args.get("postingsFormats");
  }

  @Override
  public CodecProvider create(final IndexSchema schema) {
    final Map<String,PostingsFormat> map = new HashMap<String,PostingsFormat>();
    // add standard and pulsing
    PostingsFormat p = new Lucene40PostingsFormat();
    map.put(p.name, p);
    p = new PulsingPostingsFormat(new Lucene40PostingsBaseFormat(), 1);
    map.put(p.name, p);
    
    if (formats != null) {
      for (Object format : formats.getAll("name")) {
          try {
            Class<? extends PostingsFormat> clazz = Class.forName((String)format).asSubclass(PostingsFormat.class);
            PostingsFormat fmt = clazz.newInstance();
            map.put(fmt.name, fmt);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
      }
    }
    
    final String defaultFormat = this.defaultFormat;
    return new CoreCodecProvider() {
      @Override
      public Codec getDefaultCodec() {
        return new Lucene40Codec() {
          @Override
          public String getPostingsFormatForField(String field) {
            final SchemaField fieldOrNull = schema.getFieldOrNull(field);
            if (fieldOrNull == null) {
              throw new IllegalArgumentException("no such field " + field);
            }
            String postingsFormatName = fieldOrNull.getType().getPostingsFormat();
            if (postingsFormatName != null) {
              return postingsFormatName;
            }
            return defaultFormat;
          }

          @Override
          public PostingsFormat getPostingsFormat(String formatName) {
            return map.get(formatName);
          }
        };
      }
    };
  }
}

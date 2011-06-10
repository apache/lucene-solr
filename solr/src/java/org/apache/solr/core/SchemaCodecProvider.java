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
import java.util.Collection;
import java.util.Set;

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.SegmentInfosReader;
import org.apache.lucene.index.codecs.SegmentInfosWriter;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

/**
 * Selects a codec based on a {@link IndexSchema}. This {@link CodecProvider}
 * also supports dynamic fields such that not all field codecs need to be known
 * in advance
 */
final class SchemaCodecProvider extends CodecProvider {
  private final IndexSchema schema;
  private final CodecProvider delegate;

  SchemaCodecProvider(IndexSchema schema, CodecProvider delegate) {
    this.schema = schema;
    this.delegate = delegate;
  }

  @Override
  public Codec lookup(String name) {
    synchronized (delegate) {
      return delegate.lookup(name);
    }
  }

  @Override
  public String getFieldCodec(String name) {
    synchronized (delegate) {
      if (!delegate.hasFieldCodec(name)) {
        final SchemaField fieldOrNull = schema.getFieldOrNull(name);
        if (fieldOrNull == null) {
          throw new IllegalArgumentException("no such field " + name);
        }
        String codecName = fieldOrNull.getType().getCodec();
        if (codecName == null) {
          codecName = delegate.getDefaultFieldCodec();
        }
        delegate.setFieldCodec(name, codecName);
        return codecName;
      }
      return delegate.getFieldCodec(name);
    }
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public void register(Codec codec) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unregister(Codec codec) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<String> getAllExtensions() {
    return delegate.getAllExtensions();
  }

  @Override
  public SegmentInfosWriter getSegmentInfosWriter() {
    return delegate.getSegmentInfosWriter();
  }

  @Override
  public SegmentInfosReader getSegmentInfosReader() {
    return delegate.getSegmentInfosReader();
  }

  @Override
  public boolean equals(Object obj) {
    return delegate.equals(obj);
  }

  @Override
  public void setFieldCodec(String field, String codec) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getDefaultFieldCodec() {
    return delegate.getDefaultFieldCodec();
  }

  @Override
  public boolean isCodecRegistered(String name) {
    synchronized (delegate) {
      return delegate.isCodecRegistered(name);
    }
  }

  @Override
  public void setDefaultFieldCodec(String codec) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasFieldCodec(String fieldName) {
    synchronized (delegate) {
      if (!delegate.hasFieldCodec(fieldName)) {
        final SchemaField fieldOrNull = schema.getFieldOrNull(fieldName);
        if (fieldOrNull == null) {
          return false;
        }
        String codecName = fieldOrNull.getType().getCodec();
        if (codecName == null) {
          codecName = delegate.getDefaultFieldCodec();
        }
        delegate.setFieldCodec(fieldName, codecName);
      }
      return true;
    }
  }

  @Override
  public String toString() {
    return "SchemaCodecProvider(" + delegate.toString() + ")";
  }

  @Override
  public Set<String> listAll() {
    synchronized (delegate) {
      return delegate.listAll();
    }
  }
}

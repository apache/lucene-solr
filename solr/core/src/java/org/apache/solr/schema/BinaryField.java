/*
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
package org.apache.solr.schema;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Base64;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BinaryField extends FieldType  {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void checkSchemaField(SchemaField field) {
    super.checkSchemaField(field);
    if (field.isLarge()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Field type " + this + " is 'large'; not supported (yet)");
    }
  }

  private String toBase64String(ByteBuffer buf) {
    return Base64.byteArrayToBase64(buf.array(), buf.arrayOffset() + buf.position(), buf.limit()-buf.position());
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeStr(name, toBase64String(toObject(f)), false);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new RuntimeException("Cannot sort on a Binary field");
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    // TODO: maybe just return null?
    if (sf.multiValued()) {
      return Type.SORTED_SET_BINARY;
    } else {
      return Type.BINARY;
    }
  }

  @Override
  public String toExternal(IndexableField f) {
    return toBase64String(toObject(f));
  }

  @Override
  public ByteBuffer toObject(IndexableField f) {
    BytesRef bytes = f.binaryValue();
    return  ByteBuffer.wrap(bytes.bytes, bytes.offset, bytes.length);
  }

  @Override
  public IndexableField createField(SchemaField field, Object val) {
    if (val == null) return null;
    if (!field.stored()) {
      log.trace("Ignoring unstored binary field: {}", field);
      return null;
    }
    byte[] buf = null;
    int offset = 0, len = 0;
    if (val instanceof byte[]) {
      buf = (byte[]) val;
      len = buf.length;
    } else if (val instanceof ByteBuffer && ((ByteBuffer)val).hasArray()) {
      ByteBuffer byteBuf = (ByteBuffer) val;
      buf = byteBuf.array();
      offset = byteBuf.arrayOffset() + byteBuf.position();
      len = byteBuf.limit() - byteBuf.position();
    } else {
      String strVal = val.toString();
      //the string has to be a base64 encoded string
      buf = Base64.base64ToByteArray(strVal);
      offset = 0;
      len = buf.length;
    }

    return new org.apache.lucene.document.StoredField(field.getName(), buf, offset, len);
  }

  @Override
  public Object toNativeType(Object val) {
    if (val instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) val);
    } else if (val instanceof CharSequence) {
      final CharSequence valAsCharSequence = (CharSequence) val;
      return ByteBuffer.wrap(Base64.base64ToByteArray(valAsCharSequence.toString()));
    }
    return super.toNativeType(val);
  }
}

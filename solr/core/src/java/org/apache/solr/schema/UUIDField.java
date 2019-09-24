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
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.update.processor.UUIDUpdateProcessorFactory; // jdoc
/**
 * <p>
 * This FieldType accepts UUID string values, as well as the special value 
 * of "NEW" which triggers generation of a new random UUID.
 * </p>
 * <p>
 * <b>NOTE:</b> Configuring a <code>UUIDField</code> 
 * instance with a default value of "<code>NEW</code>" is not advisable for 
 * most users when using SolrCloud (and not possible if the UUID value is 
 * configured as the unique key field) since the result will be that each 
 * replica of each document will get a unique UUID value.  
 * Using {@link UUIDUpdateProcessorFactory} to generate UUID values when 
 * documents are added is recommended instead.
 * </p>
 * 
 * @see UUID#toString
 * @see UUID#randomUUID
 *
 */
public class UUIDField extends StrField {
  private static final String NEW = "NEW";
  private static final char DASH='-';

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);

    // Tokenizing makes no sense
    restrictProps(TOKENIZED);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean reverse) {
    return getStringSort(field, reverse);
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f)
      throws IOException {
    writer.writeStr(name, f.stringValue(), false);
  }

  /**
   * Generates a UUID if val is either null, empty or "NEW".
   * 
   * Otherwise it behaves much like a StrField but checks that the value given
   * is indeed a valid UUID.
   * 
   * @param val The value of the field
   * @see org.apache.solr.schema.FieldType#toInternal(java.lang.String)
   */
  @Override
  public String toInternal(String val) {
    if (val == null || 0==val.length() || NEW.equals(val)) {
      return UUID.randomUUID().toString().toLowerCase(Locale.ROOT);
    } else {
      // we do some basic validation if 'val' looks like an UUID
      if (val.length() != 36 || val.charAt(8) != DASH || val.charAt(13) != DASH
          || val.charAt(18) != DASH || val.charAt(23) != DASH) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Invalid UUID String: '" + val + "'");
      }

      return val.toLowerCase(Locale.ROOT);
    }
  }

  public String toInternal(UUID uuid) {
    return uuid.toString().toLowerCase(Locale.ROOT);
  }

  @Override
  public UUID toObject(IndexableField f) {
    return UUID.fromString(f.stringValue());
  }

  @Override
  public Object toNativeType(Object val) {
    if (val instanceof CharSequence) {
      return UUID.fromString(val.toString());
    }
    return val;
  }
}

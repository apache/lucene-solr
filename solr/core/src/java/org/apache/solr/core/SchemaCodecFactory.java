package org.apache.solr.core;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50Codec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.plugin.SolrCoreAware;

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

/**
 * Per-field CodecFactory implementation, extends Lucene's 
 * and returns postings format implementations according to the 
 * schema configuration.
 * @lucene.experimental
 */
public class SchemaCodecFactory extends CodecFactory implements SolrCoreAware {
  private Codec codec;
  private volatile SolrCore core;
  
  // TODO: we need to change how solr does this?
  // rather than a string like "Direct" you need to be able to pass parameters
  // and everything to a field in the schema, e.g. we should provide factories for 
  // the Lucene's core formats (Memory, Direct, ...) and such.
  //
  // So I think a FieldType should return PostingsFormat, not a String.
  // how it constructs this from the XML... i don't care.

  @Override
  public void inform(SolrCore core) {
    this.core = core;
  }

  @Override
  public void init(NamedList args) {
    super.init(args);
    codec = new Lucene50Codec() {
      @Override
      public PostingsFormat getPostingsFormatForField(String field) {
        final SchemaField schemaField = core.getLatestSchema().getFieldOrNull(field);
        if (schemaField != null) {
          String postingsFormatName = schemaField.getType().getPostingsFormat();
          if (postingsFormatName != null) {
            return PostingsFormat.forName(postingsFormatName);
          }
        }
        return super.getPostingsFormatForField(field);
      }
      @Override
      public DocValuesFormat getDocValuesFormatForField(String field) {
        final SchemaField schemaField = core.getLatestSchema().getFieldOrNull(field);
        if (schemaField != null) {
          String docValuesFormatName = schemaField.getType().getDocValuesFormat();
          if (docValuesFormatName != null) {
            return DocValuesFormat.forName(docValuesFormatName);
          }
        }
        return super.getDocValuesFormatForField(field);
      }
    };
  }

  @Override
  public Codec getCodec() {
    assert core != null : "inform must be called first";
    return codec;
  }
}

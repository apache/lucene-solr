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
package org.apache.solr.morphlines.cell;

import java.util.Collection;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.solr.handler.extraction.SolrContentHandlerFactory;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.tika.metadata.Metadata;

/**
 * {@link SolrContentHandler} and associated factory that trims field values on output.
 * This prevents exceptions on parsing integer fields inside Solr server.
 */
public class TrimSolrContentHandlerFactory extends SolrContentHandlerFactory {

  public TrimSolrContentHandlerFactory(Collection<String> dateFormats) {
    super(dateFormats);
  }

  @Override
  public SolrContentHandler createSolrContentHandler(Metadata metadata, SolrParams params, IndexSchema schema) {
    return new TrimSolrContentHandler(metadata, params, schema, dateFormats);
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class TrimSolrContentHandler extends SolrContentHandler {
    
    public TrimSolrContentHandler(Metadata metadata, SolrParams params, IndexSchema schema, Collection<String> dateFormats) {
      super(metadata, params, schema, dateFormats);
    }

    @Override
    protected String transformValue(String val, SchemaField schemaField) {
      return super.transformValue(val, schemaField).trim();
    }
  }
}

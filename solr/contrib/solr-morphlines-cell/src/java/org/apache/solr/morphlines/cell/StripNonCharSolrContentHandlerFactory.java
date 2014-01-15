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
 * {@link SolrContentHandler} and associated factory that strips non-characters and trims on output.
 * This prevents exceptions on parsing integer fields inside Solr server.
 */
public class StripNonCharSolrContentHandlerFactory extends SolrContentHandlerFactory {

  public StripNonCharSolrContentHandlerFactory(Collection<String> dateFormats) {
    super(dateFormats);
  }

  @Override
  public SolrContentHandler createSolrContentHandler(Metadata metadata, SolrParams params, IndexSchema schema) {
    return new StripNonCharSolrContentHandler(metadata, params, schema, dateFormats);
  }


  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class StripNonCharSolrContentHandler extends SolrContentHandler {

    public StripNonCharSolrContentHandler(Metadata metadata, SolrParams params, IndexSchema schema, Collection<String> dateFormats) {
      super(metadata, params, schema, dateFormats);
    }

    /**
     * Strip all non-characters, which can cause SolrReducer problems if present.
     * This is borrowed from Apache Nutch.
     */
    private static String stripNonCharCodepoints(String input) {
      StringBuilder stripped = new StringBuilder(input.length());
      char ch;
      for (int i = 0; i < input.length(); i++) {
        ch = input.charAt(i);
        // Strip all non-characters http://unicode.org/cldr/utility/list-unicodeset.jsp?a=[:Noncharacter_Code_Point=True:]
        // and non-printable control characters except tabulator, new line and carriage return
        if (ch % 0x10000 != 0xffff && // 0xffff - 0x10ffff range step 0x10000
          ch % 0x10000 != 0xfffe && // 0xfffe - 0x10fffe range
          (ch <= 0xfdd0 || ch >= 0xfdef) && // 0xfdd0 - 0xfdef
          (ch > 0x1F || ch == 0x9 || ch == 0xa || ch == 0xd)) {
          stripped.append(ch);
        }
      }
      return stripped.toString();
    }

    @Override
    protected String transformValue(String val, SchemaField schemaField) {
      String ret = super.transformValue(val, schemaField).trim();
      ret = stripNonCharCodepoints(ret);
      return ret;
    }
  }
}

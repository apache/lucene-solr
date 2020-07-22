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
package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.JavaBinCodec;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 *
 * @since solr 1.3
 */
public class BinaryResponseParser extends ResponseParser {
  public static final String BINARY_CONTENT_TYPE = "application/octet-stream";

  protected JavaBinCodec.StringCache stringCache;

  public BinaryResponseParser setStringCache(JavaBinCodec.StringCache cache) {
    this.stringCache = cache;
    return this;
  }

  @Override
  public String getWriterType() {
    return "javabin";
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public NamedList<Object> processResponse(InputStream body, String encoding) {
    try {
      return (NamedList<Object>) createCodec().unmarshal(body);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "parsing error", e);

    }
  }

  protected JavaBinCodec createCodec() {
    return new JavaBinCodec(null, stringCache);
  }

  @Override
  public String getContentType() {
    return BINARY_CONTENT_TYPE;
  }

  @Override
  public String getVersion() {
    return "2";
  }

  @Override
  public NamedList<Object> processResponse(Reader reader) {
    throw new RuntimeException("Cannot handle character stream");
  }
}

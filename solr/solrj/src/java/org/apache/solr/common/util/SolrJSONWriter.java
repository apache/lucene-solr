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

package org.apache.solr.common.util;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;

/**
 * Use this to serialize an object into Json. This only supports standard Objects
 * and not the server-side Objects
 */
public class SolrJSONWriter implements JsonTextWriter {
  // indent up to 40 spaces
  static final char[] indentChars = new char[81];

  static {
    Arrays.fill(indentChars, ' ');
    indentChars[0] = '\n';  // start with a newline
  }

  final protected String namedListStyle;
  final FastWriter writer;
  protected int level;
  protected boolean doIndent;

  public SolrJSONWriter(Writer writer) {
    this(writer, JSON_NL_MAP);
  }

  public SolrJSONWriter(Writer writer, String namedListStyle) {
    this.writer = writer == null ? null : FastWriter.wrap(writer);
    this.namedListStyle = namedListStyle;
  }

  public SolrJSONWriter writeObj(Object o) throws IOException {
    writeVal(null, o);
    return this;
  }

  /**
   * done with all writing
   */
  public void close() throws IOException {
    if (writer != null) writer.flushBuffer();
  }


  @Override
  public String getNamedListStyle() {
    return namedListStyle;
  }

  @Override
  public void _writeChar(char c) throws IOException {
    writer.write(c);
  }

  @Override
  public void _writeStr(String s) throws IOException {
    writer.write(s);
  }


  public void setLevel(int level) {
    this.level = level;
  }

  public int level() {
    return level;
  }

  @Override
  public int incLevel() {
    return ++level;
  }

  @Override
  public int decLevel() {
    return --level;
  }

  @Override
  public SolrJSONWriter setIndent(boolean doIndent) {
    this.doIndent = doIndent;
    return this;
  }

  @Override
  public boolean doIndent() {
    return doIndent;
  }

  @Override
  public Writer getWriter() {
    return writer;
  }

}

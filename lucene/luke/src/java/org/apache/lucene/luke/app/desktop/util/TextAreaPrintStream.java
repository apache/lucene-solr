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

package org.apache.lucene.luke.app.desktop.util;

import javax.swing.JTextArea;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

/** PrintStream for text areas */
public final class TextAreaPrintStream extends PrintStream {

  private final ByteArrayOutputStream baos;

  private final JTextArea textArea;

  public TextAreaPrintStream(JTextArea textArea) throws UnsupportedEncodingException {
    super(new ByteArrayOutputStream(), false, StandardCharsets.UTF_8.name()); // TODO: replace by Charset in Java 11
    this.baos = (ByteArrayOutputStream) out;
    this.textArea = textArea;
    baos.reset();
  }

  @Override
  public void flush() {
    try {
      textArea.append(baos.toString(StandardCharsets.UTF_8.name())); // TODO: replace by Charset in Java 11
    } catch (UnsupportedEncodingException e) {
      setError();
    } finally {
      baos.reset();
    }
  }
}

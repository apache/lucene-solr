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

import org.slf4j.Logger;

import javax.swing.JTextArea;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** PrintStream for text areas */
public final class TextAreaPrintStream extends PrintStream {

  private Logger log;

  private ByteArrayOutputStream baos;

  private JTextArea textArea;

  public TextAreaPrintStream(JTextArea textArea, ByteArrayOutputStream baos, Charset charset, Logger log) throws UnsupportedEncodingException {
    super(baos, false, charset.name());
    this.baos = baos;
    this.textArea = textArea;
    this.log = log;
    baos.reset();
  }

  @Override
  public void println(String s) {
    try {
      baos.write(s.getBytes(StandardCharsets.UTF_8));
      baos.write('\n');
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
  }

  @Override
  public void flush() {
    try {
      textArea.append(baos.toString(StandardCharsets.UTF_8.name()));
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    } finally {
      baos.reset();
    }
  }
}

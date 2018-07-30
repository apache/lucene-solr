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

package org.apache.lucene.luke.app.util;

import javafx.scene.control.TextArea;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

public class TextAreaPrintStream extends PrintStream {


  private Logger logger;

  private ByteArrayOutputStream baos;

  private TextArea textArea;

  public TextAreaPrintStream(TextArea textArea, ByteArrayOutputStream baos, Logger logger) {
    super(baos, false);
    this.baos = baos;
    this.textArea = textArea;
    this.logger = logger;
    baos.reset();
    textArea.selectEnd();
  }

  @Override
  public void println(String s) {
    try {
      baos.write(s.getBytes());
      baos.write('\n');
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }
  }

  @Override
  public void flush() {
    try {
      textArea.selectEnd();
      textArea.appendText(baos.toString(StandardCharsets.UTF_8.name()));
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    } finally {
      baos.reset();
    }
  }

}


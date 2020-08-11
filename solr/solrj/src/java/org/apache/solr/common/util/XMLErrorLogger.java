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

import org.slf4j.Logger;

import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import javax.xml.transform.ErrorListener;
import javax.xml.transform.TransformerException;
import javax.xml.stream.Location;
import javax.xml.stream.XMLReporter;

public final class XMLErrorLogger implements ErrorHandler,ErrorListener,XMLReporter {

  private final Logger log;

  public XMLErrorLogger(Logger log) {
    this.log = log;
  }

  // ErrorHandler

  @Override
  public void warning(SAXParseException e) {
    log.warn("XML parse warning in '{}', line {}, column {}:", e.getSystemId(), e.getLineNumber(), e.getColumnNumber(), e);
  }

  @Override
  public void error(SAXParseException e) throws SAXException {
    throw e;
  }

  @Override
  public void fatalError(SAXParseException e) throws SAXException {
    throw e;
  }

  // ErrorListener

  @Override
  public void warning(TransformerException e) {
    log.warn(e.getMessageAndLocation());
  }

  @Override
  public void error(TransformerException e) throws TransformerException {
    throw e;
  }

  @Override
  public void fatalError(TransformerException e) throws TransformerException {
    throw e;
  }

  // XMLReporter

  @Override
  public void report(String message, String errorType, Object relatedInformation, Location loc) {
    final StringBuilder sb = new StringBuilder("XML parser reported ").append(errorType);
    if (loc !=  null) {
      sb.append(" in \"").append(loc.getSystemId()).append("\", line ")
        .append(loc.getLineNumber()).append(", column ").append(loc.getColumnNumber());
    }
    log.warn("{}", sb.append(": ").append(message));
  }

}

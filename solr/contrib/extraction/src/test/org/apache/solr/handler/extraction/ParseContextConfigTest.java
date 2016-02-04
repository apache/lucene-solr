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
package org.apache.solr.handler.extraction;

import javax.xml.parsers.DocumentBuilderFactory;
import java.nio.file.Paths;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ParseContextConfigTest extends SolrTestCaseJ4 {

  public void  testAll() throws Exception {
    Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    Element entries = document.createElement("entries");
    Element entry = document.createElement("entry");


    entry.setAttribute("class", "org.apache.tika.parser.pdf.PDFParserConfig");
    entry.setAttribute("impl", "org.apache.tika.parser.pdf.PDFParserConfig");

    Element property = document.createElement("property");

    property.setAttribute("name", "extractInlineImages");
    property.setAttribute("value", "true");
    entry.appendChild(property);
    entries.appendChild(entry);

    ParseContext parseContext = new ParseContextConfig(new SolrResourceLoader(Paths.get(".")), entries).create();

    PDFParserConfig pdfParserConfig = parseContext.get(PDFParserConfig.class);

    assertEquals(true, pdfParserConfig.getExtractInlineImages());
  }

}

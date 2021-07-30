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

package org.apache.solr.handler.designer;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.loader.CSVLoaderBase;
import org.apache.solr.handler.loader.JsonLoader;
import org.apache.solr.handler.loader.XMLLoader;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.SafeXMLParsing;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import static org.apache.solr.common.params.CommonParams.JSON_MIME;
import static org.apache.solr.handler.loader.CSVLoaderBase.SEPARATOR;

public class DefaultSampleDocumentsLoader implements SampleDocumentsLoader {
  public static final String CSV_MULTI_VALUE_DELIM_PARAM = "csvMultiValueDelimiter";
  private static final int MAX_STREAM_SIZE = (5 * 1024 * 1024);
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static byte[] streamAsBytes(final InputStream in) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[1024];
    int r;
    try {
      while ((r = in.read(buf)) != -1) baos.write(buf, 0, r);
    } finally {
      in.close();
    }
    return baos.toByteArray();
  }

  @Override
  public SampleDocuments parseDocsFromStream(SolrParams params, ContentStream stream, final int maxDocsToLoad) throws IOException {
    final String contentType = stream.getContentType();
    if (contentType == null) {
      return SampleDocuments.NONE;
    }

    if (params == null) {
      params = new ModifiableSolrParams();
    }

    Long streamSize = stream.getSize();
    if (streamSize != null && streamSize > MAX_STREAM_SIZE) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Sample is too big! " + MAX_STREAM_SIZE + " bytes is the max upload size for sample documents.");
    }

    String fileSource = "paste";
    if ("file".equals(stream.getName())) {
      fileSource = stream.getSourceInfo() != null ? stream.getSourceInfo() : "file";
    }

    byte[] uploadedBytes = streamAsBytes(stream.getStream());
    // recheck the upload size in case the stream returned null for getSize
    if (uploadedBytes.length > MAX_STREAM_SIZE) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Sample is too big! " + MAX_STREAM_SIZE + " bytes is the max upload size for sample documents.");
    }
    // use a byte stream for the parsers in case they need to re-parse using a different strategy
    // e.g. JSON vs. JSON lines or different CSV strategies ...
    ContentStreamBase.ByteArrayStream byteStream = new ContentStreamBase.ByteArrayStream(uploadedBytes, fileSource, contentType);
    String charset = ContentStreamBase.getCharsetFromContentType(stream.getContentType());
    if (charset == null) {
      charset = ContentStreamBase.DEFAULT_CHARSET;
    }

    List<SolrInputDocument> docs = null;
    if (stream.getSize() > 0) {
      if (contentType.contains(JSON_MIME)) {
        docs = loadJsonDocs(params, byteStream, maxDocsToLoad);
      } else if (contentType.contains("text/xml") || contentType.contains("application/xml")) {
        docs = loadXmlDocs(params, byteStream, maxDocsToLoad);
      } else if (contentType.contains("text/csv") || contentType.contains("application/csv")) {
        docs = loadCsvDocs(params, fileSource, uploadedBytes, charset, maxDocsToLoad);
      } else if (contentType.contains("text/plain") || contentType.contains("application/octet-stream")) {
        docs = loadJsonLines(params, byteStream, maxDocsToLoad);
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, contentType + " not supported yet!");
      }

      if (docs != null && maxDocsToLoad > 0 && docs.size() > maxDocsToLoad) {
        docs = docs.subList(0, maxDocsToLoad);
      }
    }

    return new SampleDocuments(docs, contentType, fileSource);
  }

  protected List<SolrInputDocument> loadCsvDocs(SolrParams params, String source, byte[] streamBytes, String charset, final int maxDocsToLoad) throws IOException {
    ContentStream stream;
    if (params.get(SEPARATOR) == null) {
      String csvStr = new String(streamBytes, charset);
      char sep = detectTSV(csvStr);
      ModifiableSolrParams modifiableSolrParams = new ModifiableSolrParams(params);
      modifiableSolrParams.set(SEPARATOR, String.valueOf(sep));
      params = modifiableSolrParams;
      stream = new ContentStreamBase.StringStream(csvStr, "text/csv");
    } else {
      stream = new ContentStreamBase.ByteArrayStream(streamBytes, source, "text/csv");
    }
    return (new SampleCSVLoader(new CSVRequest(params), maxDocsToLoad)).loadDocs(stream);
  }

  @SuppressWarnings("unchecked")
  protected List<SolrInputDocument> loadJsonLines(SolrParams params, ContentStreamBase.ByteArrayStream stream, final int maxDocsToLoad) throws IOException {
    List<Map<String, Object>> docs = new LinkedList<>();
    try (Reader r = stream.getReader()) {
      BufferedReader br = new BufferedReader(r);
      String line;
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (!line.isEmpty() && line.startsWith("{") && line.endsWith("}")) {
          Object jsonLine = ObjectBuilder.getVal(new JSONParser(line));
          if (jsonLine instanceof Map) {
            docs.add((Map<String, Object>) jsonLine);
          }
        }
        if (maxDocsToLoad > 0 && docs.size() == maxDocsToLoad) {
          break;
        }
      }
    }

    return docs.stream().map(JsonLoader::buildDoc).collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  protected List<SolrInputDocument> loadJsonDocs(SolrParams params, ContentStreamBase.ByteArrayStream stream, final int maxDocsToLoad) throws IOException {
    Object json;
    try (Reader r = stream.getReader()) {
      json = ObjectBuilder.getVal(new JSONParser(r));
    }
    if (json == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Expected at least 1 JSON doc in the request body!");
    }

    List<Map<String, Object>> docs;
    if (json instanceof List) {
      // list of docs
      docs = (List<Map<String, Object>>) json;
    } else if (json instanceof Map) {
      // single doc ... see if this is a json lines file
      boolean isJsonLines = false;
      String charset = ContentStreamBase.getCharsetFromContentType(stream.getContentType());
      String jsonStr = new String(streamAsBytes(stream.getStream()), charset != null ? charset : ContentStreamBase.DEFAULT_CHARSET);
      String[] lines = jsonStr.split("\n");
      if (lines.length > 1) {
        for (String line : lines) {
          line = line.trim();
          if (!line.isEmpty() && line.startsWith("{") && line.endsWith("}")) {
            isJsonLines = true;
            break;
          }
        }
      }
      if (isJsonLines) {
        docs = loadJsonLines(lines);
      } else {
        docs = Collections.singletonList((Map<String, Object>) json);
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Expected one or more JSON docs in the request body!");
    }
    if (maxDocsToLoad > 0 && docs.size() > maxDocsToLoad) {
      docs = docs.subList(0, maxDocsToLoad);
    }
    return docs.stream().map(JsonLoader::buildDoc).collect(Collectors.toList());
  }

  protected List<SolrInputDocument> loadXmlDocs(SolrParams params, ContentStreamBase.ByteArrayStream stream, final int maxDocsToLoad) throws IOException {
    String xmlString = readInputAsString(stream.getStream()).trim();
    List<SolrInputDocument> docs;
    if (xmlString.contains("<add>") && xmlString.contains("<doc>")) {
      XMLInputFactory inputFactory = XMLInputFactory.newInstance();
      inputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
      inputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
      XMLStreamReader parser = null;
      try {
        parser = inputFactory.createXMLStreamReader(new StringReader(xmlString));
        docs = parseXmlDocs(parser, maxDocsToLoad);
      } catch (XMLStreamException e) {
        throw new IOException(e);
      } finally {
        if (parser != null) {
          try {
            parser.close();
          } catch (XMLStreamException ignore) {
          }
        }
      }
    } else {
      Document xmlDoc;
      try {
        xmlDoc = SafeXMLParsing.parseUntrustedXML(log, xmlString);
      } catch (SAXException e) {
        throw new IOException(e);
      }
      Element root = xmlDoc.getDocumentElement();
      // TODO: support other types of XML here
      throw new IOException("TODO: XML documents with root " + root.getTagName() + " not supported yet!");
    }
    return docs;
  }

  protected List<SolrInputDocument> parseXmlDocs(XMLStreamReader parser, final int maxDocsToLoad) throws XMLStreamException {
    List<SolrInputDocument> docs = new LinkedList<>();
    XMLLoader loader = new XMLLoader().init(null);
    while (true) {
      final int event;
      try {
        event = parser.next();
      } catch (java.util.NoSuchElementException noSuchElementException) {
        return docs;
      }
      switch (event) {
        case XMLStreamConstants.END_DOCUMENT:
          parser.close();
          return docs;
        case XMLStreamConstants.START_ELEMENT:
          if ("doc".equals(parser.getLocalName())) {
            SolrInputDocument doc = loader.readDoc(parser);
            if (doc != null) {
              docs.add(doc);

              if (maxDocsToLoad > 0 && docs.size() >= maxDocsToLoad) {
                parser.close();
                return docs;
              }
            }
          }
      }
    }
  }

  @SuppressWarnings("unchecked")
  protected List<Map<String, Object>> loadJsonLines(String[] lines) throws IOException {
    List<Map<String, Object>> docs = new ArrayList<>(lines.length);
    for (String line : lines) {
      line = line.trim();
      if (!line.isEmpty() && line.startsWith("{") && line.endsWith("}")) {
        Object jsonLine = ObjectBuilder.getVal(new JSONParser(line));
        if (jsonLine instanceof Map) {
          docs.add((Map<String, Object>) jsonLine);
        }
      }
    }
    return docs;
  }

  protected String readInputAsString(InputStream in) throws IOException {
    return new String(streamAsBytes(in), StandardCharsets.UTF_8);
  }

  protected char detectTSV(String csvStr) {
    char sep = ',';
    int endOfFirstLine = csvStr.indexOf('\n');
    if (endOfFirstLine != -1) {
      int commas = 0;
      int tabs = 0;
      for (char value : csvStr.substring(0, endOfFirstLine).toCharArray()) {
        if (value == ',') {
          ++commas;
        } else if (value == '\t') {
          ++tabs;
        }
      }
      if (tabs >= commas) {
        sep = '\t';
      }
    }
    return sep;
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public void init(NamedList args) {

  }

  private static class NoOpUpdateRequestProcessor extends UpdateRequestProcessor {
    NoOpUpdateRequestProcessor() {
      super(null);
    }
  }

  private static class CSVRequest extends SolrQueryRequestBase {
    CSVRequest(SolrParams params) {
      super(null, params);
    }
  }

  private static class SampleCSVLoader extends CSVLoaderBase {
    List<SolrInputDocument> docs = new LinkedList<>();
    CSVRequest req;
    int maxDocsToLoad;
    String multiValueDelimiter;

    SampleCSVLoader(CSVRequest req, int maxDocsToLoad) {
      super(req, new NoOpUpdateRequestProcessor());
      this.req = req;
      this.maxDocsToLoad = maxDocsToLoad;
      this.multiValueDelimiter = req.getParams().get(CSV_MULTI_VALUE_DELIM_PARAM);
    }

    List<SolrInputDocument> loadDocs(ContentStream stream) throws IOException {
      load(req, new SolrQueryResponse(), stream, processor);
      return docs;
    }

    @Override
    public void addDoc(int line, String[] vals) throws IOException {
      if (maxDocsToLoad > 0 && docs.size() >= maxDocsToLoad) {
        return; // just a short circuit, probably doesn't help that much
      }

      templateAdd.clear();
      SolrInputDocument doc = new SolrInputDocument();
      doAdd(line, vals, doc, templateAdd);
      if (templateAdd.solrDoc != null) {
        if (multiValueDelimiter != null) {
          for (SolrInputField field : templateAdd.solrDoc.values()) {
            if (field.getValueCount() == 1) {
              Object value = field.getFirstValue();
              if (value instanceof String) {
                String[] splitValue = ((String) value).split(multiValueDelimiter);
                if (splitValue.length > 1) {
                  field.setValue(Arrays.asList(splitValue));
                }
              }
            }
          }
        }
        docs.add(templateAdd.solrDoc);
      }
    }
  }
}

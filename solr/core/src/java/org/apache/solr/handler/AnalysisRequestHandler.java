package org.apache.solr.handler;
/**
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

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeReflector;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @deprecated Use {@link org.apache.solr.handler.DocumentAnalysisRequestHandler} instead.
 **/
@Deprecated
public class AnalysisRequestHandler extends RequestHandlerBase {

  public static Logger log = LoggerFactory.getLogger(AnalysisRequestHandler.class);

  private XMLInputFactory inputFactory;

  @Override
  public void init(NamedList args) {
    super.init(args);

    inputFactory = XMLInputFactory.newInstance();
    try {
      // The java 1.6 bundled stax parser (sjsxp) does not currently have a thread-safe
      // XMLInputFactory, as that implementation tries to cache and reuse the
      // XMLStreamReader.  Setting the parser-specific "reuse-instance" property to false
      // prevents this.
      // All other known open-source stax parsers (and the bea ref impl)
      // have thread-safe factories.
      inputFactory.setProperty("reuse-instance", Boolean.FALSE);
    }
    catch (IllegalArgumentException ex) {
      // Other implementations will likely throw this exception since "reuse-instance"
      // isimplementation specific.
      log.debug("Unable to set the 'reuse-instance' property for the input factory: " + inputFactory);
    }
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams params = req.getParams();
    Iterable<ContentStream> streams = req.getContentStreams();
    if (streams != null) {
      for (ContentStream stream : req.getContentStreams()) {
        InputStream is = null;
        XMLStreamReader parser = null;
        try {
          is = stream.getStream();
          final String charset = ContentStreamBase.getCharsetFromContentType(stream.getContentType());
          parser = (charset == null) ?
            inputFactory.createXMLStreamReader(is) : inputFactory.createXMLStreamReader(is, charset);

          NamedList<Object> result = processContent(parser, req.getSchema());
          rsp.add("response", result);
        } finally {
          if (parser != null) parser.close();
          IOUtils.closeQuietly(is);
        }
      }
    }
  }

  NamedList<Object> processContent(XMLStreamReader parser,
                                   IndexSchema schema) throws XMLStreamException, IOException {
    NamedList<Object> result = new SimpleOrderedMap<Object>();
    while (true) {
      int event = parser.next();
      switch (event) {
        case XMLStreamConstants.END_DOCUMENT: {
          parser.close();
          return result;
        }
        case XMLStreamConstants.START_ELEMENT: {
          String currTag = parser.getLocalName();
          if ("doc".equals(currTag)) {
            log.trace("Tokenizing doc...");

            SolrInputDocument doc = readDoc(parser);
            SchemaField uniq = schema.getUniqueKeyField();
            NamedList<NamedList<NamedList<Object>>> theTokens = new SimpleOrderedMap<NamedList<NamedList<Object>>>();
            result.add(doc.getFieldValue(uniq.getName()).toString(), theTokens);
            for (String name : doc.getFieldNames()) {
              FieldType ft = schema.getFieldType(name);
              Analyzer analyzer = ft.getAnalyzer();
              Collection<Object> vals = doc.getFieldValues(name);
              for (Object val : vals) {
                Reader reader = new StringReader(val.toString());
                TokenStream tstream = analyzer.tokenStream(name, reader);
                NamedList<NamedList<Object>> tokens = getTokens(tstream);
                theTokens.add(name, tokens);
              }
            }
          }
          break;
        }
      }
    }
  }
  
  // a static mapping of the reflected attribute keys to the names used in Solr 1.3/1.4
  static Map<String,String> ATTRIBUTE_MAPPING = Collections.unmodifiableMap(new HashMap<String,String>() {{
    put(CharTermAttribute.class.getName() + "#term", "value");
    put(OffsetAttribute.class.getName() + "#startOffset", "start");
    put(OffsetAttribute.class.getName() + "#endOffset", "end");
    put(PositionIncrementAttribute.class.getName() + "#positionIncrement", "posInc");
    put(TypeAttribute.class.getName() + "#type", "type");
  }});

  static NamedList<NamedList<Object>> getTokens(TokenStream tstream) throws IOException {
    // outer is namedList since order of tokens is important
    NamedList<NamedList<Object>> tokens = new NamedList<NamedList<Object>>();
    
    while (tstream.incrementToken()) {
      final NamedList<Object> token = new SimpleOrderedMap<Object>();
      tokens.add("token", token);
      tstream.reflectWith(new AttributeReflector() {
        public void reflect(Class<? extends Attribute> attClass, String key, Object value) {
          String k = attClass.getName() + '#' + key;
          // map keys for "standard attributes":
          if (ATTRIBUTE_MAPPING.containsKey(k)) {
            k = ATTRIBUTE_MAPPING.get(k);
          }
          token.add(k, value);
        }
      });
    }
    return tokens;
  }

  SolrInputDocument readDoc(XMLStreamReader parser) throws XMLStreamException {
    SolrInputDocument doc = new SolrInputDocument();

    StringBuilder text = new StringBuilder();
    String name = null;
    String attrName = "";
    float boost = 1.0f;
    boolean isNull = false;
    while (true) {
      int event = parser.next();
      switch (event) {
        // Add everything to the text
        case XMLStreamConstants.SPACE:
        case XMLStreamConstants.CDATA:
        case XMLStreamConstants.CHARACTERS:
          text.append(parser.getText());
          break;

        case XMLStreamConstants.END_ELEMENT:
          if ("doc".equals(parser.getLocalName())) {
            return doc;
          } else if ("field".equals(parser.getLocalName())) {
            if (!isNull) {
              doc.addField(name, text.toString(), boost);
              boost = 1.0f;
            }
          }
          break;

        case XMLStreamConstants.START_ELEMENT:
          text.setLength(0);
          String localName = parser.getLocalName();
          if (!"field".equals(localName)) {
            log.warn("unexpected XML tag doc/" + localName);
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    "unexpected XML tag doc/" + localName);
          }

          String attrVal = "";
          for (int i = 0; i < parser.getAttributeCount(); i++) {
            attrName = parser.getAttributeLocalName(i);
            attrVal = parser.getAttributeValue(i);
            if ("name".equals(attrName)) {
              name = attrVal;
            }
          }
          break;
      }
    }
  }


  //////////////////////// SolrInfoMBeans methods //////////////////////
  @Override
  public String getDescription() {
    return "Provide Analysis of text";
  }

  @Override
  public String getVersion() {
    return "$Revision:$";
  }

  @Override
  public String getSourceId() {
    return "$Id:$";
  }

  @Override
  public String getSource() {
    return "$URL:$";
  }

}

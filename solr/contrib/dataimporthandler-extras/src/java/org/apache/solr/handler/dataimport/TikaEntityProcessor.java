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
package org.apache.solr.handler.dataimport;

import org.apache.commons.io.IOUtils;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.EmptyParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.parser.html.IdentityHtmlMapper;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ContentHandlerDecorator;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;
import static org.apache.solr.handler.dataimport.DataImporter.COLUMN;
import static org.apache.solr.handler.dataimport.XPathEntityProcessor.URL;
/**
 * <p>An implementation of {@link EntityProcessor} which reads data from rich docs
 * using <a href="http://tika.apache.org/">Apache Tika</a>
 *
 * <p>To index latitude/longitude data that might
 * be extracted from a file's metadata, identify
 * the geo field for this information with this attribute:
 * <code>spatialMetadataField</code>
 *
 * @since solr 3.1
 */
public class TikaEntityProcessor extends EntityProcessorBase {
  private static Parser EMPTY_PARSER = new EmptyParser();
  private TikaConfig tikaConfig;
  private String format = "text";
  private boolean done = false;
  private boolean extractEmbedded = false;
  private String parser;
  static final String AUTO_PARSER = "org.apache.tika.parser.AutoDetectParser";
  private String htmlMapper;
  private String spatialMetadataField;

  @Override
  public void init(Context context) {
    super.init(context);
    done = false;
  }

  @Override
  protected void firstInit(Context context) {
    super.firstInit(context);
    // See similar code in ExtractingRequestHandler.inform
    try {
      String tikaConfigLoc = context.getResolvedEntityAttribute("tikaConfig");
      if (tikaConfigLoc == null) {
        ClassLoader classLoader = context.getSolrCore().getResourceLoader().getClassLoader();
        try (InputStream is = classLoader.getResourceAsStream("solr-default-tika-config.xml")) {
          tikaConfig = new TikaConfig(is);
        }
      } else {
        File configFile = new File(tikaConfigLoc);
        if (configFile.isAbsolute()) {
          tikaConfig = new TikaConfig(configFile);
        } else { // in conf/
          try (InputStream is = context.getSolrCore().getResourceLoader().openResource(tikaConfigLoc)) {
            tikaConfig = new TikaConfig(is);
          }
        }
      }
    } catch (Exception e) {
      wrapAndThrow(SEVERE, e,"Unable to load Tika Config");
    }

    String extractEmbeddedString = context.getResolvedEntityAttribute("extractEmbedded");
    if ("true".equals(extractEmbeddedString)) {
      extractEmbedded = true;
    }
    format = context.getResolvedEntityAttribute("format");
    if(format == null)
      format = "text";
    if (!"html".equals(format) && !"xml".equals(format) && !"text".equals(format)&& !"none".equals(format) )
      throw new DataImportHandlerException(SEVERE, "'format' can be one of text|html|xml|none");

    htmlMapper = context.getResolvedEntityAttribute("htmlMapper");
    if (htmlMapper == null)
      htmlMapper = "default";
    if (!"default".equals(htmlMapper) && !"identity".equals(htmlMapper))
      throw new DataImportHandlerException(SEVERE, "'htmlMapper', if present, must be 'default' or 'identity'");

    parser = context.getResolvedEntityAttribute("parser");
    if(parser == null) {
      parser = AUTO_PARSER;
    }

    spatialMetadataField = context.getResolvedEntityAttribute("spatialMetadataField");
  }

  @Override
  public Map<String, Object> nextRow() {
    if(done) return null;
    Map<String, Object> row = new HashMap<>();
    @SuppressWarnings({"unchecked"})
    DataSource<InputStream> dataSource = context.getDataSource();
    InputStream is = dataSource.getData(context.getResolvedEntityAttribute(URL));
    ContentHandler contentHandler = null;
    Metadata metadata = new Metadata();
    StringWriter sw = new StringWriter();
    try {
      if ("html".equals(format)) {
        contentHandler = getHtmlHandler(sw);
      } else if ("xml".equals(format)) {
        contentHandler = getXmlContentHandler(sw);
      } else if ("text".equals(format)) {
        contentHandler = getTextContentHandler(sw);
      } else if("none".equals(format)){
        contentHandler = new DefaultHandler();        
      }
    } catch (TransformerConfigurationException e) {
      wrapAndThrow(SEVERE, e, "Unable to create content handler");
    }
    Parser tikaParser = null;
    if(parser.equals(AUTO_PARSER)){
      tikaParser = new AutoDetectParser(tikaConfig);
    } else {
      tikaParser = context.getSolrCore().getResourceLoader().newInstance(parser, Parser.class);
    }
    try {
        ParseContext context = new ParseContext();
        if ("identity".equals(htmlMapper)){
          context.set(HtmlMapper.class, IdentityHtmlMapper.INSTANCE);
        }
        if (extractEmbedded) {
          context.set(Parser.class, tikaParser);
        } else {
          context.set(Parser.class, EMPTY_PARSER);
        }
        tikaParser.parse(is, contentHandler, metadata , context);
    } catch (Exception e) {
      if(SKIP.equals(onError)) {
        throw new DataImportHandlerException(DataImportHandlerException.SKIP_ROW,
            "Document skipped :" + e.getMessage());
      }
      wrapAndThrow(SEVERE, e, "Unable to read content");
    }
    IOUtils.closeQuietly(is);
    for (Map<String, String> field : context.getAllEntityFields()) {
      if (!"true".equals(field.get("meta"))) continue;
      String col = field.get(COLUMN);
      String s = metadata.get(col);
      if (s != null) row.put(col, s);
    }
    if(!"none".equals(format) ) row.put("text", sw.toString());
    tryToAddLatLon(metadata, row);
    done = true;
    return row;
  }

  private void tryToAddLatLon(Metadata metadata, Map<String, Object> row) {
    if (spatialMetadataField == null) return;
    String latString = metadata.get(Metadata.LATITUDE);
    String lonString = metadata.get(Metadata.LONGITUDE);
    if (latString != null && lonString != null) {
      row.put(spatialMetadataField, String.format(Locale.ROOT, "%s,%s", latString, lonString));
    }
  }

  private static ContentHandler getHtmlHandler(Writer writer)
          throws TransformerConfigurationException {
    SAXTransformerFactory factory = (SAXTransformerFactory)
            TransformerFactory.newInstance();
    TransformerHandler handler = factory.newTransformerHandler();
    handler.getTransformer().setOutputProperty(OutputKeys.METHOD, "html");
    handler.setResult(new StreamResult(writer));
    return new ContentHandlerDecorator(handler) {
      @Override
      public void startElement(
              String uri, String localName, String name, Attributes atts)
              throws SAXException {
        if (XHTMLContentHandler.XHTML.equals(uri)) {
          uri = null;
        }
        if (!"head".equals(localName)) {
          super.startElement(uri, localName, name, atts);
        }
      }

      @Override
      public void endElement(String uri, String localName, String name)
              throws SAXException {
        if (XHTMLContentHandler.XHTML.equals(uri)) {
          uri = null;
        }
        if (!"head".equals(localName)) {
          super.endElement(uri, localName, name);
        }
      }

      @Override
      public void startPrefixMapping(String prefix, String uri) {/*no op*/ }

      @Override
      public void endPrefixMapping(String prefix) {/*no op*/ }
    };
  }

  private static ContentHandler getTextContentHandler(Writer writer) {
    return new BodyContentHandler(writer);
  }

  private static ContentHandler getXmlContentHandler(Writer writer)
          throws TransformerConfigurationException {
    SAXTransformerFactory factory = (SAXTransformerFactory)
            TransformerFactory.newInstance();
    TransformerHandler handler = factory.newTransformerHandler();
    handler.getTransformer().setOutputProperty(OutputKeys.METHOD, "xml");
    handler.setResult(new StreamResult(writer));
    return handler;
  }

}

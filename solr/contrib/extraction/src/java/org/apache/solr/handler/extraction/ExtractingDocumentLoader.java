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

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.Locale;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.HttpHeaders;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.DefaultParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.PasswordProvider;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.tika.sax.xpath.Matcher;
import org.apache.tika.sax.xpath.MatchingContentHandler;
import org.apache.tika.sax.xpath.XPathParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;


/**
 * The class responsible for loading extracted content into Solr.
 *
 **/
public class ExtractingDocumentLoader extends ContentStreamLoader {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Extract Only supported format
   */
  public static final String TEXT_FORMAT = "text";
  /**
   * Extract Only supported format.  Default
   */
  public static final String XML_FORMAT = "xml";
  /**
   * XHTML XPath parser.
   */
  private static final XPathParser PARSER =
          new XPathParser("xhtml", XHTMLContentHandler.XHTML);

  final SolrCore core;
  final SolrParams params;
  final UpdateRequestProcessor processor;
  final boolean ignoreTikaException;
  protected AutoDetectParser autoDetectParser;

  private final AddUpdateCommand templateAdd;

  protected TikaConfig config;
  protected ParseContextConfig parseContextConfig;
  protected SolrContentHandlerFactory factory;

  public ExtractingDocumentLoader(SolrQueryRequest req, UpdateRequestProcessor processor,
                           TikaConfig config, ParseContextConfig parseContextConfig,
                                  SolrContentHandlerFactory factory) {
    this.params = req.getParams();
    this.core = req.getCore();
    this.config = config;
    this.parseContextConfig = parseContextConfig;
    this.processor = processor;

    templateAdd = new AddUpdateCommand(req);
    templateAdd.overwrite = params.getBool(UpdateParams.OVERWRITE, true);
    templateAdd.commitWithin = params.getInt(UpdateParams.COMMIT_WITHIN, -1);

    //this is lightweight
    autoDetectParser = new AutoDetectParser(config);
    this.factory = factory;
    
    ignoreTikaException = params.getBool(ExtractingParams.IGNORE_TIKA_EXCEPTION, false);
  }


  /**
   * this must be MT safe... may be called concurrently from multiple threads.
   *
   */
  void doAdd(SolrContentHandler handler, AddUpdateCommand template)
          throws IOException {
    template.solrDoc = handler.newDocument();
    processor.processAdd(template);
  }

  void addDoc(SolrContentHandler handler) throws IOException {
    templateAdd.clear();
    doAdd(handler, templateAdd);
  }

  @SuppressWarnings({"deprecation", "unchecked", "rawtypes"})
  @Override
  public void load(SolrQueryRequest req, SolrQueryResponse rsp,
      ContentStream stream, UpdateRequestProcessor processor) throws Exception {
    Parser parser = null;
    String streamType = req.getParams().get(ExtractingParams.STREAM_TYPE, null);
    if (streamType != null) {
      //Cache?  Parsers are lightweight to construct and thread-safe, so I'm told
      MediaType mt = MediaType.parse(streamType.trim().toLowerCase(Locale.ROOT));
      parser = new DefaultParser(config.getMediaTypeRegistry()).getParsers().get(mt);
    } else {
      parser = autoDetectParser;
    }
    if (parser != null) {
      Metadata metadata = new Metadata();

      // If you specify the resource name (the filename, roughly) with this parameter,
      // then Tika can make use of it in guessing the appropriate MIME type:
      String resourceName = req.getParams().get(ExtractingParams.RESOURCE_NAME, null);
      if (resourceName != null) {
        metadata.add(TikaMetadataKeys.RESOURCE_NAME_KEY, resourceName);
      }
      // Provide stream's content type as hint for auto detection
      if(stream.getContentType() != null) {
        metadata.add(HttpHeaders.CONTENT_TYPE, stream.getContentType());
      }

      InputStream inputStream = null;
      try {
        inputStream = stream.getStream();
        metadata.add(ExtractingMetadataConstants.STREAM_NAME, stream.getName());
        metadata.add(ExtractingMetadataConstants.STREAM_SOURCE_INFO, stream.getSourceInfo());
        metadata.add(ExtractingMetadataConstants.STREAM_SIZE, String.valueOf(stream.getSize()));
        metadata.add(ExtractingMetadataConstants.STREAM_CONTENT_TYPE, stream.getContentType());
        // HtmlParser and TXTParser regard Metadata.CONTENT_ENCODING in metadata
        String charset = ContentStreamBase.getCharsetFromContentType(stream.getContentType());
        if(charset != null){
          metadata.add(HttpHeaders.CONTENT_ENCODING, charset);
        }

        String xpathExpr = params.get(ExtractingParams.XPATH_EXPRESSION);
        boolean extractOnly = params.getBool(ExtractingParams.EXTRACT_ONLY, false);
        SolrContentHandler handler = factory.createSolrContentHandler(metadata, params, req.getSchema());
        ContentHandler parsingHandler = handler;

        StringWriter writer = null;
        org.apache.xml.serialize.BaseMarkupSerializer serializer = null;
        if (extractOnly == true) {
          String extractFormat = params.get(ExtractingParams.EXTRACT_FORMAT, "xml");
          writer = new StringWriter();
          if (extractFormat.equals(TEXT_FORMAT)) {
            serializer = new org.apache.xml.serialize.TextSerializer();
            serializer.setOutputCharStream(writer);
            serializer.setOutputFormat(new org.apache.xml.serialize.OutputFormat("Text", "UTF-8", true));
          } else {
            serializer = new org.apache.xml.serialize.XMLSerializer(writer, new org.apache.xml.serialize.OutputFormat("XML", "UTF-8", true));
          }
          if (xpathExpr != null) {
            Matcher matcher =
                    PARSER.parse(xpathExpr);
            serializer.startDocument();//The MatchingContentHandler does not invoke startDocument.  See http://tika.markmail.org/message/kknu3hw7argwiqin
            parsingHandler = new MatchingContentHandler(serializer, matcher);
          } else {
            parsingHandler = serializer;
          }
        } else if (xpathExpr != null) {
          Matcher matcher =
                  PARSER.parse(xpathExpr);
          parsingHandler = new MatchingContentHandler(handler, matcher);
        } //else leave it as is

        try{
          //potentially use a wrapper handler for parsing, but we still need the SolrContentHandler for getting the document.
          ParseContext context = parseContextConfig.create();


          context.set(Parser.class, parser);
          context.set(HtmlMapper.class, MostlyPassthroughHtmlMapper.INSTANCE);

          // Password handling
          RegexRulesPasswordProvider epp = new RegexRulesPasswordProvider();
          String pwMapFile = params.get(ExtractingParams.PASSWORD_MAP_FILE);
          if(pwMapFile != null && pwMapFile.length() > 0) {
            InputStream is = req.getCore().getResourceLoader().openResource(pwMapFile);
            if(is != null) {
              log.debug("Password file supplied: "+pwMapFile);
              epp.parse(is);
            }
          }
          context.set(PasswordProvider.class, epp);
          String resourcePassword = params.get(ExtractingParams.RESOURCE_PASSWORD);
          if(resourcePassword != null) {
            epp.setExplicitPassword(resourcePassword);
            log.debug("Literal password supplied for file "+resourceName);
          }
          parser.parse(inputStream, parsingHandler, metadata, context);
        } catch (TikaException e) {
          if(ignoreTikaException)
            log.warn(new StringBuilder("skip extracting text due to ").append(e.getLocalizedMessage())
                .append(". metadata=").append(metadata.toString()).toString());
          else
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
        if (extractOnly == false) {
          addDoc(handler);
        } else {
          //serializer is not null, so we need to call endDoc on it if using xpath
          if (xpathExpr != null){
            serializer.endDocument();
          }
          rsp.add(stream.getName(), writer.toString());
          writer.close();
          String[] names = metadata.names();
          NamedList metadataNL = new NamedList();
          for (int i = 0; i < names.length; i++) {
            String[] vals = metadata.getValues(names[i]);
            metadataNL.add(names[i], vals);
          }
          rsp.add(stream.getName() + "_metadata", metadataNL);
        }
      } catch (SAXException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } finally {
        IOUtils.closeQuietly(inputStream);
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Stream type of " + streamType + " didn't match any known parsers.  Please supply the " + ExtractingParams.STREAM_TYPE + " parameter.");
    }
  }

  public static class MostlyPassthroughHtmlMapper implements HtmlMapper {
    public static final HtmlMapper INSTANCE = new MostlyPassthroughHtmlMapper();

    /** 
     * Keep all elements and their content.
     *  
     * Apparently &lt;SCRIPT&gt; and &lt;STYLE&gt; elements are blocked elsewhere
     */
    @Override
    public boolean isDiscardElement(String name) {     
      return false;
    }

    /** Lowercases the attribute name */
    @Override
    public String mapSafeAttribute(String elementName, String attributeName) {
      return attributeName.toLowerCase(Locale.ENGLISH);
    }

    /**
     * Lowercases the element name, but returns null for &lt;BR&gt;,
     * which suppresses the start-element event for lt;BR&gt; tags.
     * This also suppresses the &lt;BODY&gt; tags because those
     * are handled internally by Tika's XHTMLContentHandler.
     */
    @Override
    public String mapSafeElement(String name) {
      String lowerName = name.toLowerCase(Locale.ROOT);
      return (lowerName.equals("br") || lowerName.equals("body")) ? null : lowerName;
    }
   }
 }
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
package org.apache.solr.morphlines.cell;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IllformedLocaleException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.io.Closeables;
import com.typesafe.config.Config;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.handler.extraction.ExtractingParams;
import org.apache.solr.handler.extraction.ExtractionDateUtil;
import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.solr.handler.extraction.SolrContentHandlerFactory;
import org.apache.solr.morphlines.solr.SolrLocator;
import org.apache.solr.schema.IndexSchema;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.tika.sax.xpath.Matcher;
import org.apache.tika.sax.xpath.MatchingContentHandler;
import org.apache.tika.sax.xpath.XPathParser;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * Command that pipes the first attachment of a record into one of the given Tika parsers, then maps
 * the Tika output back to a record using SolrCell.
 * <p>
 * The Tika parser is chosen from the configurable list of parsers, depending on the MIME type
 * specified in the input record. Typically, this requires an upstream DetectMimeTypeBuilder
 * in a prior command.
 */
public final class SolrCellBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("solrCell");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new SolrCell(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class SolrCell extends AbstractParser {
    
    private final IndexSchema schema;
    private final List<String> dateFormats;
    private final String xpathExpr;
    private final List<Parser> parsers = new ArrayList<>();
    private final SolrContentHandlerFactory solrContentHandlerFactory;
    private final Locale locale;
    
    private final SolrParams solrParams;
    private final Map<MediaType, Parser> mediaTypeToParserMap;
    
    private static final XPathParser PARSER = new XPathParser("xhtml", XHTMLContentHandler.XHTML);
        
    public static final String ADDITIONAL_SUPPORTED_MIME_TYPES = "additionalSupportedMimeTypes";
    
    public SolrCell(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      
      Config solrLocatorConfig = getConfigs().getConfig(config, "solrLocator");
      SolrLocator locator = new SolrLocator(solrLocatorConfig, context);
      LOG.debug("solrLocator: {}", locator);
      this.schema = Objects.requireNonNull(locator.getIndexSchema());
      if (LOG.isTraceEnabled()) {
        LOG.trace("Solr schema: \n" + schema.getFields().entrySet().stream()
                .sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).map(Object::toString)
                .collect(Collectors.joining("\n")));
      }

      ListMultimap<String, String> cellParams = ArrayListMultimap.create();
      String uprefix = getConfigs().getString(config, ExtractingParams.UNKNOWN_FIELD_PREFIX, null);
      if (uprefix != null) {
        cellParams.put(ExtractingParams.UNKNOWN_FIELD_PREFIX, uprefix);
      }
      for (String capture : getConfigs().getStringList(config, ExtractingParams.CAPTURE_ELEMENTS, Collections.<String>emptyList())) {
        cellParams.put(ExtractingParams.CAPTURE_ELEMENTS, capture);
      }
      Config fmapConfig = getConfigs().getConfig(config, "fmap", null);
      if (fmapConfig != null) {
        for (Map.Entry<String, Object> entry : new Configs().getEntrySet(fmapConfig)) {
          cellParams.put(ExtractingParams.MAP_PREFIX + entry.getKey(), entry.getValue().toString());
        }
      }
      String captureAttributes = getConfigs().getString(config, ExtractingParams.CAPTURE_ATTRIBUTES, null);
      if (captureAttributes != null) {
        cellParams.put(ExtractingParams.CAPTURE_ATTRIBUTES, captureAttributes);
      }
      String lowerNames = getConfigs().getString(config, ExtractingParams.LOWERNAMES, null);
      if (lowerNames != null) {
        cellParams.put(ExtractingParams.LOWERNAMES, lowerNames);
      }
      String defaultField = getConfigs().getString(config, ExtractingParams.DEFAULT_FIELD, null);
      if (defaultField != null) {
        cellParams.put(ExtractingParams.DEFAULT_FIELD, defaultField);
      }
      xpathExpr = getConfigs().getString(config, ExtractingParams.XPATH_EXPRESSION, null);
      if (xpathExpr != null) {
        cellParams.put(ExtractingParams.XPATH_EXPRESSION, xpathExpr);
      }
      
      this.dateFormats = getConfigs().getStringList(config, "dateFormats", new ArrayList<>(ExtractionDateUtil.DEFAULT_DATE_FORMATS));
      
      String handlerStr = getConfigs().getString(config, "solrContentHandlerFactory", TrimSolrContentHandlerFactory.class.getName());
      Class<? extends SolrContentHandlerFactory> factoryClass;
      try {
        factoryClass = Class.forName(handlerStr).asSubclass(SolrContentHandlerFactory.class);
      } catch (ClassNotFoundException cnfe) {
        throw new MorphlineCompilationException("Could not find class "
          + handlerStr + " to use for " + "solrContentHandlerFactory", config, cnfe);
      }
      this.solrContentHandlerFactory = getSolrContentHandlerFactory(factoryClass, dateFormats, config);

      this.locale = getLocale(getConfigs().getString(config, "locale", null));
      
      this.mediaTypeToParserMap = new HashMap<>();
      //MimeTypes mimeTypes = MimeTypes.getDefaultMimeTypes(); // FIXME getMediaTypeRegistry.normalize() 

      List<? extends Config> parserConfigs = getConfigs().getConfigList(config, "parsers");
      for (Config parserConfig : parserConfigs) {
        String parserClassName = getConfigs().getString(parserConfig, "parser");
        
        Object obj;
        try {
          obj = Class.forName(parserClassName).newInstance();
        } catch (Throwable e) {
          throw new MorphlineCompilationException("Cannot instantiate Tika parser: " + parserClassName, config, e);
        }
        if (!(obj instanceof Parser)) {
          throw new MorphlineCompilationException("Tika parser " + obj.getClass().getName()
              + " must be an instance of class " + Parser.class.getName(), config);
        }
        Parser parser = (Parser) obj;
        this.parsers.add(parser);

        List<String> mediaTypes = getConfigs().getStringList(parserConfig, SUPPORTED_MIME_TYPES, Collections.<String>emptyList());
        for (String mediaTypeStr : mediaTypes) {
          MediaType mediaType = parseMediaType(mediaTypeStr);
          addSupportedMimeType(mediaTypeStr);
          this.mediaTypeToParserMap.put(mediaType, parser);
        }
        
        if (!parserConfig.hasPath(SUPPORTED_MIME_TYPES)) {
          for (MediaType mediaType : parser.getSupportedTypes(new ParseContext())) {
            mediaType = mediaType.getBaseType();
            addSupportedMimeType(mediaType.toString());
            this.mediaTypeToParserMap.put(mediaType, parser);
          }        
          List<String> extras = getConfigs().getStringList(parserConfig, ADDITIONAL_SUPPORTED_MIME_TYPES, Collections.<String>emptyList());
          for (String mediaTypeStr : extras) {
            MediaType mediaType = parseMediaType(mediaTypeStr);
            addSupportedMimeType(mediaTypeStr);
            this.mediaTypeToParserMap.put(mediaType, parser);            
          }
        }
      }
      //LOG.info("mediaTypeToParserMap="+mediaTypeToParserMap);

      Map<String, String[]> tmp = new HashMap<>();
      for (Map.Entry<String,Collection<String>> entry : cellParams.asMap().entrySet()) {
        tmp.put(entry.getKey(), entry.getValue().toArray(new String[entry.getValue().size()]));
      }
      this.solrParams = new MultiMapSolrParams(tmp);
      validateArguments();
    }
    
    @Override
    protected boolean doProcess(Record record, InputStream inputStream) {
      Parser parser = detectParser(record);
      if (parser == null) {
        return false;
      }
      
      ParseContext parseContext = new ParseContext();
      parseContext.set(Locale.class, locale);
      
      Metadata metadata = new Metadata();
      for (Entry<String, Object> entry : record.getFields().entries()) {
        metadata.add(entry.getKey(), entry.getValue().toString());
      }

      SolrContentHandler handler = solrContentHandlerFactory.createSolrContentHandler(metadata, solrParams, schema);
      try {
        inputStream = TikaInputStream.get(inputStream);

        ContentHandler parsingHandler = handler;

        // String xpathExpr = "/xhtml:html/xhtml:body/xhtml:div/descendant:node()";
        if (xpathExpr != null) {
          Matcher matcher = PARSER.parse(xpathExpr);
          parsingHandler = new MatchingContentHandler(parsingHandler, matcher);
        }

        try {
          parser.parse(inputStream, parsingHandler, metadata, parseContext);
        } catch (IOException | TikaException | SAXException e) {
          throw new MorphlineRuntimeException("Cannot parse", e);
        }
      } finally {
        if (inputStream != null) {
          Closeables.closeQuietly(inputStream);
        }
      }
      
      SolrInputDocument doc = handler.newDocument();
      LOG.debug("solr doc: {}", doc);      
      Record outputRecord = toRecord(doc);
      return getChild().process(outputRecord);
    }

    private Parser detectParser(Record record) {
      if (!hasAtLeastOneMimeType(record)) {
        return null;
      }
      String mediaTypeStr = (String) record.getFirstValue(Fields.ATTACHMENT_MIME_TYPE); //ExtractingParams.STREAM_TYPE);
      assert mediaTypeStr != null;
      
      MediaType mediaType = parseMediaType(mediaTypeStr).getBaseType();
      Parser parser = mediaTypeToParserMap.get(mediaType); // fast path
      if (parser != null) {
        return parser;
      }
      // wildcard matching
      for (Map.Entry<MediaType, Parser> entry : mediaTypeToParserMap.entrySet()) {
        if (isMediaTypeMatch(mediaType, entry.getKey())) {
          return entry.getValue();
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("No supported MIME type parser found for " + Fields.ATTACHMENT_MIME_TYPE + "=" + mediaTypeStr);
      }
      return null;
    }
    
    private boolean hasAtLeastOneMimeType(Record record) {
      if (!record.getFields().containsKey(Fields.ATTACHMENT_MIME_TYPE)) {
        LOG.debug("Command failed because of missing MIME type for record: {}", record);
        return false;
      }  
      return true;
    }

    private MediaType parseMediaType(String mediaTypeStr) {
      MediaType mediaType = MediaType.parse(mediaTypeStr.trim().toLowerCase(Locale.ROOT));
      return mediaType.getBaseType();
    };
        
    /** Returns true if mediaType falls withing the given range (pattern), false otherwise */
    private boolean isMediaTypeMatch(MediaType mediaType, MediaType rangePattern) {
      String WILDCARD = "*";
      String rangePatternType = rangePattern.getType();
      String rangePatternSubtype = rangePattern.getSubtype();
      return (rangePatternType.equals(WILDCARD) || rangePatternType.equals(mediaType.getType()))
          && (rangePatternSubtype.equals(WILDCARD) || rangePatternSubtype.equals(mediaType.getSubtype()));
    }
    
    private static SolrContentHandlerFactory getSolrContentHandlerFactory(
        Class<? extends SolrContentHandlerFactory> factoryClass, Collection<String> dateFormats, Config config) {
      try {
        return factoryClass.getConstructor(Collection.class).newInstance(dateFormats);
      } catch (NoSuchMethodException nsme) {
        throw new MorphlineCompilationException("Unable to find valid constructor of type "
          + factoryClass.getName() + " for creating SolrContentHandler", config, nsme);
      } catch (Exception e) {
        throw new MorphlineCompilationException("Unexpected exception when trying to create SolrContentHandlerFactory of type "
          + factoryClass.getName(), config, e);
      }
    }

    private Record toRecord(SolrInputDocument doc) {
      Record record = new Record();
      for (Entry<String, SolrInputField> entry : doc.entrySet()) {
        record.getFields().putAll(entry.getKey(), entry.getValue().getValues());        
      }
      return record;
    }
    
    @SuppressForbidden(reason = "Usage of outdated locale parsing with Locale#toString() because of backwards compatibility")
    private Locale getLocale(String name) {
      if (name == null) {
        return Locale.ROOT;
      }
      for (Locale locale : Locale.getAvailableLocales()) {
        if (locale.toString().equals(name)) {
          return locale;
        }
      }
      try {
        return new Locale.Builder().setLanguageTag(name).build();
      } catch (IllformedLocaleException ex) {
        throw new MorphlineCompilationException("Malformed / non-existent locale: " + name, getConfig(), ex);
      }
    }
  }

}

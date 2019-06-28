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

import java.lang.invoke.MethodHandles;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


/**
 * The class responsible for handling Tika events and translating them into {@link org.apache.solr.common.SolrInputDocument}s.
 * <B>This class is not thread-safe.</B>
 * <p>
 * This class cannot be reused, you have to create a new instance per document!
 * <p>
 * User's may wish to override this class to provide their own functionality.
 *
 * @see org.apache.solr.handler.extraction.SolrContentHandlerFactory
 * @see org.apache.solr.handler.extraction.ExtractingRequestHandler
 * @see org.apache.solr.handler.extraction.ExtractingDocumentLoader
 */
public class SolrContentHandler extends DefaultHandler implements ExtractingParams {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String contentFieldName = "content";

  protected final SolrInputDocument document;

  protected final Metadata metadata;
  protected final SolrParams params;
  protected final StringBuilder catchAllBuilder = new StringBuilder(2048);
  protected final IndexSchema schema;
  protected final Map<String, StringBuilder> fieldBuilders;
  private final Deque<StringBuilder> bldrStack = new ArrayDeque<>();

  protected final boolean captureAttribs;
  protected final boolean lowerNames;
  
  protected final String unknownFieldPrefix;
  protected final String defaultField;

  private final boolean literalsOverride;
  
  private Set<String> literalFieldNames = null;


  public SolrContentHandler(Metadata metadata, SolrParams params, IndexSchema schema) {
    this.document = new SolrInputDocument();
    this.metadata = metadata;
    this.params = params;
    this.schema = schema;

    this.lowerNames = params.getBool(LOWERNAMES, false);
    this.captureAttribs = params.getBool(CAPTURE_ATTRIBUTES, false);
    this.literalsOverride = params.getBool(LITERALS_OVERRIDE, true);
    this.unknownFieldPrefix = params.get(UNKNOWN_FIELD_PREFIX, "");
    this.defaultField = params.get(DEFAULT_FIELD, "");
    
    String[] captureFields = params.getParams(CAPTURE_ELEMENTS);
    if (captureFields != null && captureFields.length > 0) {
      fieldBuilders = new HashMap<>();
      for (int i = 0; i < captureFields.length; i++) {
        fieldBuilders.put(captureFields[i], new StringBuilder());
      }
    } else {
      fieldBuilders = Collections.emptyMap();
    }
    bldrStack.add(catchAllBuilder);
  }


  /**
   * This is called by a consumer when it is ready to deal with a new SolrInputDocument.  Overriding
   * classes can use this hook to add in or change whatever they deem fit for the document at that time.
   * The base implementation adds the metadata as fields, allowing for potential remapping.
   *
   * @return The {@link org.apache.solr.common.SolrInputDocument}.
   *
   * @see #addMetadata()
   * @see #addCapturedContent()
   * @see #addContent()
   * @see #addLiterals()
   */
  public SolrInputDocument newDocument() {
    //handle the literals from the params. NOTE: This MUST be called before the others in order for literals to override other values
    addLiterals();

    //handle the metadata extracted from the document
    addMetadata();

    //add in the content
    addContent();

    //add in the captured content
    addCapturedContent();

    if (log.isDebugEnabled()) {
      log.debug("Doc: {}", document);
    }
    return document;
  }

  /**
   * Add the per field captured content to the Solr Document.  Default implementation uses the
   * {@link #fieldBuilders} info
   */
  protected void addCapturedContent() {
    for (Map.Entry<String, StringBuilder> entry : fieldBuilders.entrySet()) {
      if (entry.getValue().length() > 0) {
        String fieldName = entry.getKey();
        if (literalsOverride && literalFieldNames.contains(fieldName))
          continue;
        addField(fieldName, entry.getValue().toString(), null);      }
    }
  }

  /**
   * Add in the catch all content to the field.  Default impl. uses the {@link #contentFieldName}
   * and the {@link #catchAllBuilder}
   */
  protected void addContent() {
    if (literalsOverride && literalFieldNames.contains(contentFieldName))
      return;
    addField(contentFieldName, catchAllBuilder.toString(), null);
  }

  /**
   * Add in the literals to the document using the {@link #params} and the {@link #LITERALS_PREFIX}.
   */
  protected void addLiterals() {
    Iterator<String> paramNames = params.getParameterNamesIterator();
    literalFieldNames = new HashSet<>();
    while (paramNames.hasNext()) {
      String pname = paramNames.next();
      if (!pname.startsWith(LITERALS_PREFIX)) continue;

      String name = pname.substring(LITERALS_PREFIX.length());
      addField(name, null, params.getParams(pname));
      literalFieldNames.add(name);
    }
  }

  /**
   * Add in any metadata using {@link #metadata} as the source.
   */
  protected void addMetadata() {
    for (String name : metadata.names()) {
      if (literalsOverride && literalFieldNames.contains(name))
        continue;
      String[] vals = metadata.getValues(name);
      addField(name, null, vals);
    }
  }

  // Naming rules:
  // 1) optionally map names to nicenames (lowercase+underscores)
  // 2) execute "map" commands
  // 3) if resulting field is unknown, map it to a common prefix
  protected void addField(String fname, String fval, String[] vals) {
    if (lowerNames) {
      StringBuilder sb = new StringBuilder();
      for (int i=0; i<fname.length(); i++) {
        char ch = fname.charAt(i);
        if (!Character.isLetterOrDigit(ch)) ch='_';
        else ch=Character.toLowerCase(ch);
        sb.append(ch);
      }
      fname = sb.toString();
    }    

    String name = findMappedName(fname);
    SchemaField sf = schema.getFieldOrNull(name);
    if (sf==null && unknownFieldPrefix.length() > 0) {
      name = unknownFieldPrefix + name;
      sf = schema.getFieldOrNull(name);
    } else if (sf == null && defaultField.length() > 0 && name.equals(TikaMetadataKeys.RESOURCE_NAME_KEY) == false /*let the fall through below handle this*/){
      name = defaultField;
      sf = schema.getFieldOrNull(name);
    }

    // Arguably we should handle this as a special case. Why? Because unlike basically
    // all the other fields in metadata, this one was probably set not by Tika by in
    // ExtractingDocumentLoader.load(). You shouldn't have to define a mapping for this
    // field just because you specified a resource.name parameter to the handler, should
    // you?
    if (sf == null && unknownFieldPrefix.length()==0 && name == TikaMetadataKeys.RESOURCE_NAME_KEY) {
      return;
    }

    // normalize val params so vals.length>1
    if (vals != null && vals.length==1) {
      fval = vals[0];
      vals = null;
    }

    // single valued field with multiple values... catenate them.
    if (sf != null && !sf.multiValued() && vals != null) {
      StringBuilder builder = new StringBuilder();
      boolean first=true;
      for (String val : vals) {
        if (first) {
          first=false;
        } else {
          builder.append(' ');
        }
        builder.append(val);
      }
      fval = builder.toString();
      vals=null;
    }

    if (fval != null) {
      document.addField(name, fval);
    }

    if (vals != null) {
      for (String val : vals) {
        document.addField(name, val);
      }
    }

    // no value set - throw exception for debugging
    // if (vals==null && fval==null) throw new RuntimeException(name + " has no non-null value ");
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
    StringBuilder theBldr = fieldBuilders.get(localName);
    if (theBldr != null) {
      //we need to switch the currentBuilder
      bldrStack.add(theBldr);
    }
    if (captureAttribs == true) {
      for (int i = 0; i < attributes.getLength(); i++) {
        addField(localName, attributes.getValue(i), null);
      }
    } else {
      for (int i = 0; i < attributes.getLength(); i++) {
        bldrStack.getLast().append(' ').append(attributes.getValue(i));
      }
    }
    bldrStack.getLast().append(' ');
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    StringBuilder theBldr = fieldBuilders.get(localName);
    if (theBldr != null) {
      //pop the stack
      bldrStack.removeLast();
      assert (bldrStack.size() >= 1);
    }
    bldrStack.getLast().append(' ');
  }


  @Override
  public void characters(char[] chars, int offset, int length) throws SAXException {
    bldrStack.getLast().append(chars, offset, length);
  }

  /**
   * Treat the same as any other characters
   */
  @Override
  public void ignorableWhitespace(char[] chars, int offset, int length) throws SAXException {
    characters(chars, offset, length);
  }

  /**
   * Get the name mapping
   *
   * @param name The name to check to see if there is a mapping
   * @return The new name, if there is one, else <code>name</code>
   */
  protected String findMappedName(String name) {
    return params.get(MAP_PREFIX + name, name);
  }

}

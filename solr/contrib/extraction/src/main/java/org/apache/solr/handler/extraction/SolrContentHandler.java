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

package org.apache.solr.handler.extraction;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.schema.DateField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.text.DateFormat;
import java.util.*;


/**
 * The class responsible for handling Tika events and translating them into {@link org.apache.solr.common.SolrInputDocument}s.
 * <B>This class is not thread-safe.</B>
 * <p/>
 * <p/>
 * User's may wish to override this class to provide their own functionality.
 *
 * @see org.apache.solr.handler.extraction.SolrContentHandlerFactory
 * @see org.apache.solr.handler.extraction.ExtractingRequestHandler
 * @see org.apache.solr.handler.extraction.ExtractingDocumentLoader
 */
public class SolrContentHandler extends DefaultHandler implements ExtractingParams {
  private transient static Logger log = LoggerFactory.getLogger(SolrContentHandler.class);
  private SolrInputDocument document;

  private Collection<String> dateFormats = DateUtil.DEFAULT_DATE_FORMATS;

  private Metadata metadata;
  private SolrParams params;
  private StringBuilder catchAllBuilder = new StringBuilder(2048);
  private IndexSchema schema;
  private Map<String, StringBuilder> fieldBuilders = Collections.emptyMap();
  private LinkedList<StringBuilder> bldrStack = new LinkedList<StringBuilder>();

  private boolean captureAttribs;
  private boolean lowerNames;
  private String contentFieldName = "content";

  private String unknownFieldPrefix = "";
  private String defaultField = "";

  public SolrContentHandler(Metadata metadata, SolrParams params, IndexSchema schema) {
    this(metadata, params, schema, DateUtil.DEFAULT_DATE_FORMATS);
  }


  public SolrContentHandler(Metadata metadata, SolrParams params,
                            IndexSchema schema, Collection<String> dateFormats) {
    document = new SolrInputDocument();
    this.metadata = metadata;
    this.params = params;
    this.schema = schema;
    this.dateFormats = dateFormats;

    this.lowerNames = params.getBool(LOWERNAMES, false);
    this.captureAttribs = params.getBool(CAPTURE_ATTRIBUTES, false);
    this.unknownFieldPrefix = params.get(UNKNOWN_FIELD_PREFIX, "");
    this.defaultField = params.get(DEFAULT_FIELD, "");
    String[] captureFields = params.getParams(CAPTURE_ELEMENTS);
    if (captureFields != null && captureFields.length > 0) {
      fieldBuilders = new HashMap<String, StringBuilder>();
      for (int i = 0; i < captureFields.length; i++) {
        fieldBuilders.put(captureFields[i], new StringBuilder());
      }
    }
    bldrStack.add(catchAllBuilder);
  }


  /**
   * This is called by a consumer when it is ready to deal with a new SolrInputDocument.  Overriding
   * classes can use this hook to add in or change whatever they deem fit for the document at that time.
   * The base implementation adds the metadata as fields, allowing for potential remapping.
   *
   * @return The {@link org.apache.solr.common.SolrInputDocument}.
   */
  public SolrInputDocument newDocument() {
    float boost = 1.0f;
    //handle the metadata extracted from the document
    for (String name : metadata.names()) {
      String[] vals = metadata.getValues(name);
      addField(name, null, vals);
    }

    //handle the literals from the params
    Iterator<String> paramNames = params.getParameterNamesIterator();
    while (paramNames.hasNext()) {
      String pname = paramNames.next();
      if (!pname.startsWith(LITERALS_PREFIX)) continue;

      String name = pname.substring(LITERALS_PREFIX.length());
      addField(name, null, params.getParams(pname));
    }


    //add in the content
    addField(contentFieldName, catchAllBuilder.toString(), null);

    //add in the captured content
    for (Map.Entry<String, StringBuilder> entry : fieldBuilders.entrySet()) {
      if (entry.getValue().length() > 0) {
        addField(entry.getKey(), entry.getValue().toString(), null);
      }
    }
    if (log.isDebugEnabled()) {
      log.debug("Doc: " + document);
    }
    return document;
  }

  // Naming rules:
  // 1) optionally map names to nicenames (lowercase+underscores)
  // 2) execute "map" commands
  // 3) if resulting field is unknown, map it to a common prefix
  private void addField(String fname, String fval, String[] vals) {
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
    } else if (sf == null && defaultField.length() > 0 && name.equals(Metadata.RESOURCE_NAME_KEY) == false /*let the fall through below handle this*/){
      name = defaultField;
      sf = schema.getFieldOrNull(name);
    }

    // Arguably we should handle this as a special case. Why? Because unlike basically
    // all the other fields in metadata, this one was probably set not by Tika by in
    // ExtractingDocumentLoader.load(). You shouldn't have to define a mapping for this
    // field just because you specified a resource.name parameter to the handler, should
    // you?
    if (sf == null && unknownFieldPrefix.length()==0 && name == Metadata.RESOURCE_NAME_KEY) {
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

    float boost = getBoost(name);

    if (fval != null) {
      document.addField(name, transformValue(fval, sf), boost);
    }

    if (vals != null) {
      for (String val : vals) {
        document.addField(name, transformValue(val, sf), boost);
      }
    }

    // no value set - throw exception for debugging
    // if (vals==null && fval==null) throw new RuntimeException(name + " has no non-null value ");
  }


  @Override
  public void startDocument() throws SAXException {
    document.clear();
    catchAllBuilder.setLength(0);
    for (StringBuilder builder : fieldBuilders.values()) {
      builder.setLength(0);
    }
    bldrStack.clear();
    bldrStack.add(catchAllBuilder);
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
        bldrStack.getLast().append(attributes.getValue(i)).append(' ');
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
   * Can be used to transform input values based on their {@link org.apache.solr.schema.SchemaField}
   * <p/>
   * This implementation only formats dates using the {@link org.apache.solr.common.util.DateUtil}.
   *
   * @param val    The value to transform
   * @param schFld The {@link org.apache.solr.schema.SchemaField}
   * @return The potentially new value.
   */
  protected String transformValue(String val, SchemaField schFld) {
    String result = val;
    if (schFld != null && schFld.getType() instanceof DateField) {
      //try to transform the date
      try {
        Date date = DateUtil.parseDate(val, dateFormats);
        DateFormat df = DateUtil.getThreadLocalDateFormat();
        result = df.format(date);

      } catch (Exception e) {
        // Let the specific fieldType handle errors
        // throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid value: " + val + " for field: " + schFld, e);
      }
    }
    return result;
  }


  /**
   * Get the value of any boost factor for the mapped name.
   *
   * @param name The name of the field to see if there is a boost specified
   * @return The boost value
   */
  protected float getBoost(String name) {
    return params.getFloat(BOOST_PREFIX + name, 1.0f);
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

package org.apache.solr.handler.extraction;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.schema.DateField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TextField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.UUIDField;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.text.DateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;
import java.util.UUID;


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
  protected SolrInputDocument document;

  protected Collection<String> dateFormats = DateUtil.DEFAULT_DATE_FORMATS;

  protected Metadata metadata;
  protected SolrParams params;
  protected StringBuilder catchAllBuilder = new StringBuilder(2048);
  //private StringBuilder currentBuilder;
  protected IndexSchema schema;
  //create empty so we don't have to worry about null checks
  protected Map<String, StringBuilder> fieldBuilders = Collections.emptyMap();
  protected Stack<StringBuilder> bldrStack = new Stack<StringBuilder>();

  protected boolean ignoreUndeclaredFields = false;
  protected boolean indexAttribs = false;
  protected String defaultFieldName;

  protected String metadataPrefix = "";

  /**
   * Only access through getNextId();
   */
  private static long identifier = Long.MIN_VALUE;


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
    this.ignoreUndeclaredFields = params.getBool(IGNORE_UNDECLARED_FIELDS, false);
    this.indexAttribs = params.getBool(INDEX_ATTRIBUTES, false);
    this.defaultFieldName = params.get(DEFAULT_FIELDNAME);
    this.metadataPrefix = params.get(METADATA_PREFIX, "");
    //if there's no default field and we are intending to index, then throw an exception
    if (defaultFieldName == null && params.getBool(EXTRACT_ONLY, false) == false) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "No default field name specified");
    }
    String[] captureFields = params.getParams(CAPTURE_FIELDS);
    if (captureFields != null && captureFields.length > 0) {
      fieldBuilders = new HashMap<String, StringBuilder>();
      for (int i = 0; i < captureFields.length; i++) {
        fieldBuilders.put(captureFields[i], new StringBuilder());
      }
    }
    bldrStack.push(catchAllBuilder);
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
      name = findMappedMetadataName(name);
      SchemaField schFld = schema.getFieldOrNull(name);
      if (schFld != null) {
        boost = getBoost(name);
        if (schFld.multiValued()) {
          for (int i = 0; i < vals.length; i++) {
            String val = vals[i];
            document.addField(name, transformValue(val, schFld), boost);
          }
        } else {
          StringBuilder builder = new StringBuilder();
          for (int i = 0; i < vals.length; i++) {
            builder.append(vals[i]).append(' ');
          }
          document.addField(name, transformValue(builder.toString().trim(), schFld), boost);
        }
      } else {
        //TODO: error or log?
        if (ignoreUndeclaredFields == false) {
          // Arguably we should handle this as a special case. Why? Because unlike basically
          // all the other fields in metadata, this one was probably set not by Tika by in
          // ExtractingDocumentLoader.load(). You shouldn't have to define a mapping for this
          // field just because you specified a resource.name parameter to the handler, should
          // you?
          if (name != Metadata.RESOURCE_NAME_KEY) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid field: " + name);
          }
        }
      }
    }
    //handle the literals from the params
    Iterator<String> paramNames = params.getParameterNamesIterator();
    while (paramNames.hasNext()) {
      String name = paramNames.next();
      if (name.startsWith(LITERALS_PREFIX)) {
        String fieldName = name.substring(LITERALS_PREFIX.length());
        //no need to map names here, since they are literals from the user
        SchemaField schFld = schema.getFieldOrNull(fieldName);
        if (schFld != null) {
          String[] values = params.getParams(name);
          if (schFld.multiValued() == false && values.length > 1) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "The Field " + fieldName + " is not multivalued");
          }
          boost = getBoost(fieldName);
          for (int i = 0; i < values.length; i++) {
            //no need to transform here, b/c we can assume the user sent it in correctly
            document.addField(fieldName, values[i], boost);

          }
        } else {
          handleUndeclaredField(fieldName);
        }
      }
    }
    //add in the content
    document.addField(defaultFieldName, catchAllBuilder.toString(), getBoost(defaultFieldName));

    //add in the captured content
    for (Map.Entry<String, StringBuilder> entry : fieldBuilders.entrySet()) {
      if (entry.getValue().length() > 0) {
        String fieldName = findMappedName(entry.getKey());
        SchemaField schFld = schema.getFieldOrNull(fieldName);
        if (schFld != null) {
          document.addField(fieldName, transformValue(entry.getValue().toString(), schFld), getBoost(fieldName));
        } else {
          handleUndeclaredField(fieldName);
        }
      }
    }
    //make sure we have a unique id, if one is needed
    SchemaField uniqueField = schema.getUniqueKeyField();
    if (uniqueField != null) {
      String uniqueFieldName = uniqueField.getName();
      SolrInputField uniqFld = document.getField(uniqueFieldName);
      if (uniqFld == null) {
        String uniqId = generateId(uniqueField);
        if (uniqId != null) {
          document.addField(uniqueFieldName, uniqId);
        }
      }
    }
    if (log.isDebugEnabled()) {
      log.debug("Doc: " + document);
    }
    return document;
  }

  /**
   * Generate an ID for the document.  First try to get
   * {@link ExtractingMetadataConstants#STREAM_NAME} from the
   * {@link org.apache.tika.metadata.Metadata}, then try {@link ExtractingMetadataConstants#STREAM_SOURCE_INFO}
   * then try {@link org.apache.tika.metadata.Metadata#IDENTIFIER}.
   * If those all are null, then generate a random UUID using {@link java.util.UUID#randomUUID()}.
   *
   * @param uniqueField The SchemaField representing the unique field.
   * @return The id as a string
   */
  protected String generateId(SchemaField uniqueField) {
    //we don't have a unique field specified, so let's add one
    String uniqId = null;
    FieldType type = uniqueField.getType();
    if (type instanceof StrField || type instanceof TextField) {
      uniqId = metadata.get(ExtractingMetadataConstants.STREAM_NAME);
      if (uniqId == null) {
        uniqId = metadata.get(ExtractingMetadataConstants.STREAM_SOURCE_INFO);
      }
      if (uniqId == null) {
        uniqId = metadata.get(Metadata.IDENTIFIER);
      }
      if (uniqId == null) {
        //last chance, just create one
        uniqId = UUID.randomUUID().toString();
      }
    } else if (type instanceof UUIDField) {
      uniqId = UUID.randomUUID().toString();
    } else {
      uniqId = String.valueOf(getNextId());
    }
    return uniqId;
  }


  @Override
  public void startDocument() throws SAXException {
    document.clear();
    catchAllBuilder.setLength(0);
    for (StringBuilder builder : fieldBuilders.values()) {
      builder.setLength(0);
    }
    bldrStack.clear();
    bldrStack.push(catchAllBuilder);
  }


  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
    StringBuilder theBldr = fieldBuilders.get(localName);
    if (theBldr != null) {
      //we need to switch the currentBuilder
      bldrStack.push(theBldr);
    }
    if (indexAttribs == true) {
      for (int i = 0; i < attributes.getLength(); i++) {
        String fieldName = findMappedName(localName);
        SchemaField schFld = schema.getFieldOrNull(fieldName);
        if (schFld != null) {
          document.addField(fieldName, transformValue(attributes.getValue(i), schFld), getBoost(fieldName));
        } else {
          handleUndeclaredField(fieldName);
        }
      }
    } else {
      for (int i = 0; i < attributes.getLength(); i++) {
        bldrStack.peek().append(attributes.getValue(i)).append(' ');
      }
    }
  }

  protected void handleUndeclaredField(String fieldName) {
    if (ignoreUndeclaredFields == false) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid field: " + fieldName);
    } else {
      if (log.isInfoEnabled()) {
        log.info("Ignoring Field: " + fieldName);
      }
    }
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    StringBuilder theBldr = fieldBuilders.get(localName);
    if (theBldr != null) {
      //pop the stack
      bldrStack.pop();
      assert (bldrStack.size() >= 1);
    }


  }


  @Override
  public void characters(char[] chars, int offset, int length) throws SAXException {
    bldrStack.peek().append(chars, offset, length);
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
    if (schFld.getType() instanceof DateField) {
      //try to transform the date
      try {
        Date date = DateUtil.parseDate(val, dateFormats);
        DateFormat df = DateUtil.getThreadLocalDateFormat();
        result = df.format(date);

      } catch (Exception e) {
        //TODO: error or log?
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid value: " + val + " for field: " + schFld, e);
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

  /**
   * Get the name mapping for the metadata field.  Prepends metadataPrefix onto the returned result.
   *
   * @param name The name to check to see if there is a mapping
   * @return The new name, else <code>name</code>
   */
  protected String findMappedMetadataName(String name) {
    return metadataPrefix + params.get(MAP_PREFIX + name, name);
  }


  protected synchronized long getNextId() {
    return identifier++;
  }


}

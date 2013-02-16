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

package org.apache.solr.update;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.schema.CopyField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

/**
 *
 */


// Not thread safe - by design.  Create a new builder for each thread.
public class DocumentBuilder {
  private final IndexSchema schema;
  private Document doc;
  private HashMap<String,String> map;

  public DocumentBuilder(IndexSchema schema) {
    this.schema = schema;
  }

  public void startDoc() {
    doc = new Document();
    map = new HashMap<String,String>();
  }

  protected void addSingleField(SchemaField sfield, String val, float boost) {
    //System.out.println("###################ADDING FIELD "+sfield+"="+val);

    // we don't check for a null val ourselves because a solr.FieldType
    // might actually want to map it to something.  If createField()
    // returns null, then we don't store the field.
    List<IndexableField> fields = sfield.createFields(val, boost);
    if (!fields.isEmpty()) {
      if (!sfield.multiValued()) {
        String oldValue = map.put(sfield.getName(), val);
        if (oldValue != null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "ERROR: multiple values encountered for non multiValued field " + sfield.getName()
                  + ": first='" + oldValue + "' second='" + val + "'");
        }
      }
      // Add each field
      for (IndexableField field : fields) {
        doc.add((Field) field);
      }
    }

  }

  /**
   * Add the specified {@link org.apache.solr.schema.SchemaField} to the document.  Does not invoke the copyField mechanism.
   * @param sfield The {@link org.apache.solr.schema.SchemaField} to add
   * @param val The value to add
   * @param boost The boost factor
   *
   * @see #addField(String, String)
   * @see #addField(String, String, float)
   * @see #addSingleField(org.apache.solr.schema.SchemaField, String, float)
   */
  public void addField(SchemaField sfield, String val, float boost) {
    addSingleField(sfield,val,boost);
  }

  /**
   * Add the Field and value to the document, invoking the copyField mechanism
   * @param name The name of the field
   * @param val The value to add
   *
   * @see #addField(String, String, float)
   * @see #addField(org.apache.solr.schema.SchemaField, String, float)
   * @see #addSingleField(org.apache.solr.schema.SchemaField, String, float)
   */
  public void addField(String name, String val) {
    addField(name, val, 1.0f);
  }

  /**
   * Add the Field and value to the document with the specified boost, invoking the copyField mechanism
   * @param name The name of the field.
   * @param val The value to add
   * @param boost The boost
   *
   * @see #addField(String, String)
   * @see #addField(org.apache.solr.schema.SchemaField, String, float)
   * @see #addSingleField(org.apache.solr.schema.SchemaField, String, float)
   *
   */
  public void addField(String name, String val, float boost) {
    SchemaField sfield = schema.getFieldOrNull(name);
    if (sfield != null) {
      addField(sfield,val,boost);
    }

    // Check if we should copy this field to any other fields.
    // This could happen whether it is explicit or not.
    final List<CopyField> copyFields = schema.getCopyFieldsList(name);
    if (copyFields != null) {
      for(CopyField cf : copyFields) {
        addSingleField(cf.getDestination(), cf.getLimitedValue( val ), boost);
      }
    }

    // error if this field name doesn't match anything
    if (sfield==null && (copyFields==null || copyFields.size()==0)) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"ERROR:unknown field '" + name + "'");
    }
  }

  public void endDoc() {
  }

  // specific to this type of document builder
  public Document getDoc() throws IllegalArgumentException {
    
    // Check for all required fields -- Note, all fields with a
    // default value are defacto 'required' fields.  
    List<String> missingFields = null;
    for (SchemaField field : schema.getRequiredFields()) {
      if (doc.getField(field.getName() ) == null) {
        if (field.getDefaultValue() != null) {
          addField(doc, field, field.getDefaultValue(), 1.0f);
        } else {
          if (missingFields==null) {
            missingFields = new ArrayList<String>(1);
          }
          missingFields.add(field.getName());
        }
      }
    }
  
    if (missingFields != null) {
      StringBuilder builder = new StringBuilder();
      // add the uniqueKey if possible
      if( schema.getUniqueKeyField() != null ) {
        String n = schema.getUniqueKeyField().getName();
        String v = doc.getField( n ).stringValue();
        builder.append( "Document ["+n+"="+v+"] " );
      }
      builder.append("missing required fields: " );
      for (String field : missingFields) {
        builder.append(field);
        builder.append(" ");
      }
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, builder.toString());
    }
    
    Document ret = doc; doc=null;
    return ret;
  }


  private static void addField(Document doc, SchemaField field, Object val, float boost) {
    for (IndexableField f : field.getType().createFields(field, val, boost)) {
      if (f != null) doc.add((Field) f); // null fields are not added
    }
  }
  
  private static String getID( SolrInputDocument doc, IndexSchema schema )
  {
    String id = "";
    SchemaField sf = schema.getUniqueKeyField();
    if( sf != null ) {
      id = "[doc="+doc.getFieldValue( sf.getName() )+"] ";
    }
    return id;
  }

  /**
   * Convert a SolrInputDocument to a lucene Document.
   * 
   * This function should go elsewhere.  This builds the Document without an
   * extra Map<> checking for multiple values.  For more discussion, see:
   * http://www.nabble.com/Re%3A-svn-commit%3A-r547493---in--lucene-solr-trunk%3A-.--src-java-org-apache-solr-common--src-java-org-apache-solr-schema--src-java-org-apache-solr-update--src-test-org-apache-solr-common--tf3931539.html
   * 
   * TODO: /!\ NOTE /!\ This semantics of this function are still in flux.  
   * Something somewhere needs to be able to fill up a SolrDocument from
   * a lucene document - this is one place that may happen.  It may also be
   * moved to an independent function
   * 
   * @since solr 1.3
   */
  public static Document toDocument( SolrInputDocument doc, IndexSchema schema )
  { 
    Document out = new Document();
    final float docBoost = doc.getDocumentBoost();
    
    // Load fields from SolrDocument to Document
    for( SolrInputField field : doc ) {
      String name = field.getName();
      SchemaField sfield = schema.getFieldOrNull(name);
      boolean used = false;

      
      // Make sure it has the correct number
      if( sfield!=null && !sfield.multiValued() && field.getValueCount() > 1 ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
            "ERROR: "+getID(doc, schema)+"multiple values encountered for non multiValued field " + 
              sfield.getName() + ": " +field.getValue() );
      }
      
      float fieldBoost = field.getBoost();
      boolean applyBoost = sfield != null && sfield.indexed() && !sfield.omitNorms();

      if (applyBoost == false && fieldBoost != 1.0F) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
            "ERROR: "+getID(doc, schema)+"cannot set an index-time boost, unindexed or norms are omitted for field " + 
              sfield.getName() + ": " +field.getValue() );
      }

      // Lucene no longer has a native docBoost, so we have to multiply 
      // it ourselves 
      float compoundBoost = fieldBoost * docBoost;

      // load each field value
      boolean hasField = false;
      try {
        for( Object v : field ) {
          if( v == null ) {
            continue;
          }
          hasField = true;
          if (sfield != null) {
            used = true;
            addField(out, sfield, v, applyBoost ? compoundBoost : 1f);
          }
  
          // Check if we should copy this field value to any other fields.
          // This could happen whether it is explicit or not.
          List<CopyField> copyFields = schema.getCopyFieldsList(name);
          for (CopyField cf : copyFields) {
            SchemaField destinationField = cf.getDestination();

            final boolean destHasValues = 
              (null != out.getField(destinationField.getName()));

            // check if the copy field is a multivalued or not
            if (!destinationField.multiValued() && destHasValues) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                      "ERROR: "+getID(doc, schema)+"multiple values encountered for non multiValued copy field " +
                              destinationField.getName() + ": " + v);
            }
  
            used = true;
            
            // Perhaps trim the length of a copy field
            Object val = v;
            if( val instanceof String && cf.getMaxChars() > 0 ) {
              val = cf.getLimitedValue((String)val);
            }

            // we can't copy any boost unless the dest field is 
            // indexed & !omitNorms, but which boost we copy depends
            // on wether the dest field already contains values (we 
            // don't want to apply the compounded docBoost more then once)
            final float destBoost = 
              (destinationField.indexed() && !destinationField.omitNorms()) ?
              (destHasValues ? fieldBoost : compoundBoost) : 1.0F;
            
            addField(out, destinationField, val, destBoost);
          }
          
          // The final boost for a given field named is the product of the 
          // *all* boosts on values of that field. 
          // For multi-valued fields, we only want to set the boost on the
          // first field.
          fieldBoost = compoundBoost = 1.0f;
        }
      }
      catch( SolrException ex ) {
        throw ex;
      }
      catch( Exception ex ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
            "ERROR: "+getID(doc, schema)+"Error adding field '" + 
              field.getName() + "'='" +field.getValue()+"' msg=" + ex.getMessage(), ex );
      }
      
      // make sure the field was used somehow...
      if( !used && hasField ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
            "ERROR: "+getID(doc, schema)+"unknown field '" +name + "'");
      }
    }
    
        
    // Now validate required fields or add default values
    // fields with default values are defacto 'required'
    for (SchemaField field : schema.getRequiredFields()) {
      if (out.getField(field.getName() ) == null) {
        if (field.getDefaultValue() != null) {
          addField(out, field, field.getDefaultValue(), 1.0f);
        } 
        else {
          String msg = getID(doc, schema) + "missing required field: " + field.getName();
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, msg );
        }
      }
    }
    return out;
  }
}

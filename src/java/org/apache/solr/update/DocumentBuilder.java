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

package org.apache.solr.update;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.DateField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

/**
 * @author yonik
 * @version $Id$
 */


// Not thread safe - by design.  Create a new builder for each thread.
public class DocumentBuilder {
  private final IndexSchema schema;
  private Document doc;
  private HashMap<String,String> map = new HashMap<String,String>();

  public DocumentBuilder(IndexSchema schema) {
    this.schema = schema;
  }

  public void startDoc() {
    doc = new Document();
    map.clear();
  }

  protected void addSingleField(SchemaField sfield, String val, float boost) {
    //System.out.println("###################ADDING FIELD "+sfield+"="+val);

    // we don't check for a null val ourselves because a solr.FieldType
    // might actually want to map it to something.  If createField()
    // returns null, then we don't store the field.
    Field field = sfield.createField(val, boost);
    if (field != null) {
      if (!sfield.multiValued()) {
        String oldValue = map.put(sfield.getName(), val);
        if (oldValue != null) {
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"ERROR: multiple values encountered for non multiValued field " + sfield.getName()
                  + ": first='" + oldValue + "' second='" + val + "'");
        }
      }
      
      if( doc == null ) {
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
            "must call startDoc() before adding fields!" );
      }
       
      // field.setBoost(boost);
      doc.add(field);
    }
  }


  public void addField(SchemaField sfield, String val, float boost) {
    addSingleField(sfield,val,boost);
  }

  public void addField(String name, String val) {
    addField(name, val, 1.0f);
  }

  public void addField(String name, String val, float boost) {
    SchemaField sfield = schema.getFieldOrNull(name);
    if (sfield != null) {
      addField(sfield,val,boost);
    }

    // Check if we should copy this field to any other fields.
    // This could happen whether it is explicit or not.
    SchemaField[] destArr = schema.getCopyFields(name);
    if (destArr != null) {
      for (SchemaField destField : destArr) {
        addSingleField(destField,val,boost);
      }
    }

    // error if this field name doesn't match anything
    if (sfield==null && (destArr==null || destArr.length==0)) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"ERROR:unknown field '" + name + "'");
    }
  }

  public void setBoost(float boost) {
    doc.setBoost(boost);
  }

  public void endDoc() {
  }

  // specific to this type of document builder
  public Document getDoc() throws IllegalArgumentException {
    
    // Check for all required fields -- Note, all fields with a
    // default value are defacto 'required' fields.  
    List<String> missingFields = new ArrayList<String>( schema.getRequiredFields().size() );
    for (SchemaField field : schema.getRequiredFields()) {
      if (doc.getField(field.getName() ) == null) {
        if (field.getDefaultValue() != null) {
          doc.add( field.createField( field.getDefaultValue(), 1.0f ) );
        } else {
          missingFields.add(field.getName());
        }
      }
    }
  
    if (missingFields.size() > 0) {
      StringBuilder builder = new StringBuilder();
      // add the uniqueKey if possible
      if( schema.getUniqueKeyField() != null ) {
        String n = schema.getUniqueKeyField().getName();
        String v = doc.get( n );
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
  

  /**
   * Convert a SolrInputDocument to a lucene Document.
   * 
   * This function shoould go elsewhere.  This builds the Document without an
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
    
    // Load fields from SolrDocument to Document
    for( String name : doc.getFieldNames() ) {
      SchemaField sfield = schema.getField(name);
      Float b = doc.getBoost( name );
      float boost = (b==null) ? 1.0f : b.floatValue();
      
      // Make sure it has the correct number
      Collection<Object> vals = doc.getFieldValues( name );
      if( vals.size() > 1 && !sfield.multiValued() ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
            "ERROR: multiple values encountered for non multiValued field " + 
              sfield.getName() + ": " +vals.toString() );
      }
      
      // load each field value
      for( Object v : vals ) {
        String val = null;
        if( v instanceof Date && sfield.getType() instanceof DateField ) {
          DateField df = (DateField)sfield.getType();
          val = df.toInternal( (Date)v )+'Z';
        }
        else if (v != null) {
          val = v.toString();
        }
        out.add( sfield.createField( val, boost ) );
        
        // In lucene, the boost for a given field is the product of the 
        // document boost and *all* boosts on values of that field. 
        // For multi-valued fields, we only want to set the boost on the
        // first field.
        boost = 1.0f; 
      }
    }
    
    // Now validate required fields or add default values
    // fields with default values are defacto 'required'
    for (SchemaField field : schema.getRequiredFields()) {
      if (out.getField(field.getName() ) == null) {
        if (field.getDefaultValue() != null) {
          out.add( field.createField( field.getDefaultValue(), 1.0f ) );
        } 
        else {
          String id = schema.printableUniqueKey( out );
          String msg = "Document ["+id+"] missing required field: " + field.getName();
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, msg );
        }
      }
    }
  
    // set the full document boost
    if( doc.getBoost( null ) != null ) {
      out.setBoost( doc.getBoost( null ) );
    }  
    return out;
  }

  
  /**
   * Add fields from the solr document
   * 
   * TODO: /!\ NOTE /!\ This semantics of this function are still in flux.  
   * Something somewhere needs to be able to fill up a SolrDocument from
   * a lucene document - this is one place that may happen.  It may also be
   * moved to an independent function
   * 
   * @since solr 1.3
   */
  public SolrDocument loadStoredFields( SolrDocument doc, Document luceneDoc  )
  {
    for( Object f : luceneDoc.getFields() ) {
      Fieldable field = (Fieldable)f;
      if( field.isStored() ) {
        SchemaField sf = schema.getField( field.name() );
        if( !schema.isCopyFieldTarget( sf ) ) {
          doc.addField( field.name(), sf.getType().toObject( field ) );
        }
      }
    }
    return doc;
  }
}

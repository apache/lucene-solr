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
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.solr.common.SolrException;
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
}

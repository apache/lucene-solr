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

  private static void addField(Document doc, SchemaField field, Object val, float boost) {
    if (val instanceof IndexableField) {
      // set boost to the calculated compound boost
      ((Field)val).setBoost(boost);
      doc.add((Field)val);
      return;
    }
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
            // on whether the dest field already contains values (we
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

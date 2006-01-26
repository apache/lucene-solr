/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.core.SolrException;

import java.util.HashMap;

/**
 * @author yonik
 * @version $Id: DocumentBuilder.java,v 1.7 2005/12/02 04:31:06 yonik Exp $
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
          throw new SolrException(400,"ERROR: multiple values encountered for non multiValued field " + sfield.getName()
                  + ": first='" + oldValue + "' second='" + val + "'");
        }
      }
      // field.setBoost(boost);
      doc.add(field);
    }
  }


  public void addField(SchemaField sfield, String val, float boost) {
    addSingleField(sfield,val,boost);

    // Check if we should copy this field to any other fields.
    SchemaField[] destArr = schema.getCopyFields(sfield.getName());
    if (destArr != null) {
      for (SchemaField destField : destArr) {
        addSingleField(destField,val,boost);
      }
    }
  }

  public void addField(String name, String val) {
    SchemaField ftype = schema.getField(name);
    // fields.get(name);
    addField(ftype,val,1.0f);
  }

  public void addField(String name, String val, float boost) {
    SchemaField ftype = schema.getField(name);
    addField(ftype,val,boost);
  }

  public void setBoost(float boost) {
    doc.setBoost(boost);
  }

  public void endDoc() {
  }

  // specific to this type of document builder
  public Document getDoc() {
    Document ret = doc; doc=null;
    return ret;
  }
}

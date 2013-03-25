package org.apache.solr.schema;
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

import org.apache.lucene.search.similarities.Similarity;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.SchemaAware; // javadocs
import org.apache.solr.schema.FieldType; // javadocs
import org.apache.solr.common.params.SolrParams;

import java.util.Iterator;


/**
 * A factory interface for configuring a {@link Similarity} in the Solr 
 * schema.xml.  
 * 
 * <p>
 * Subclasses of <code>SimilarityFactory</code> which are {@link SchemaAware} 
 * must take responsibility for either consulting the similarities configured 
 * on individual field types, or generating appropriate error/warning messages 
 * if field type specific similarities exist but are being ignored.  The 
 * <code>IndexSchema</code> will provide such error checking if a 
 * non-<code>SchemaAware</code> instance of <code>SimilarityFactory</code> 
 * is used.
 * 
 * @see FieldType#getSimilarity
 */
public abstract class SimilarityFactory {
  public static final String CLASS_NAME = "class";
  
  protected SolrParams params;

  public void init(SolrParams params) { this.params = params; }
  public SolrParams getParams() { return params; }

  public abstract Similarity getSimilarity();


  private static String normalizeSPIname(String fullyQualifiedName) {
    if (fullyQualifiedName.startsWith("org.apache.lucene.") || fullyQualifiedName.startsWith("org.apache.solr.")) {
      return "solr" + fullyQualifiedName.substring(fullyQualifiedName.lastIndexOf('.'));
    }
    return fullyQualifiedName;
  }

  /** Returns a description of this field's similarity, if any */
  public SimpleOrderedMap<Object> getNamedPropertyValues() {
    String className = getClass().getName();
    if (className.startsWith("org.apache.solr.schema.IndexSchema$")) {
      // If this class is just a no-params wrapper around a similarity class, use the similarity class
      className = getSimilarity().getClass().getName();
    } else {
      // Only normalize factory names
      className = normalizeSPIname(className);
    }
    SimpleOrderedMap<Object> props = new SimpleOrderedMap<Object>();
    props.add(CLASS_NAME, normalizeSPIname(className));
    if (null != params) {
      Iterator<String> iter = params.getParameterNamesIterator();
      while (iter.hasNext()) {
        String key = iter.next();
        props.add(key, params.get(key));
      }
    }
    return props;
  }
}

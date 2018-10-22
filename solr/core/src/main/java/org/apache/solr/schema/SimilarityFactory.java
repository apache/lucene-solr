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
package org.apache.solr.schema;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.solr.common.util.SimpleOrderedMap;
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


  /** Returns a serializable description of this similarity(factory) */
  public SimpleOrderedMap<Object> getNamedPropertyValues() {
    SimpleOrderedMap<Object> props = new SimpleOrderedMap<>();
    props.add(CLASS_NAME, getClassArg());
    if (null != params) {
      Iterator<String> iter = params.getParameterNamesIterator();
      while (iter.hasNext()) {
        String key = iter.next();
        if ( ! CLASS_NAME.equals(key)) {
          props.add(key, params.get(key));
        }
      }
    }
    return props;
  }

  /**
   * @return the string used to specify the concrete class name in a serialized representation: the class arg.  
   *         If the concrete class name was not specified via a class arg, returns {@code getClass().getName()},
   *         unless this class is the anonymous similarity wrapper produced in {@link IndexSchema}, in which
   *         case the {@code getSimilarity().getClass().getName()} is returned.
   */
  public String getClassArg() {
    if (null != params) {
      String className = params.get(CLASS_NAME);
      if (null != className) {
        return className;
      }
    }
    String className = getClass().getName(); 
    if (className.startsWith("org.apache.solr.schema.IndexSchema$")) {
      // If this class is just a no-params wrapper around a similarity class, use the similarity class
      className = getSimilarity().getClass().getName();
    }
    return className; 
  }
}

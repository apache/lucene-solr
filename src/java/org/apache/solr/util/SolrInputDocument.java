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

package org.apache.solr.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Represent the field and boost information needed to construct and index
 * a Lucene Document.  Like the SolrDocument, the field values need to
 * match those specified in schema.xml 
 * 
 * @author ryan
 * @version $Id$
 * @since solr 1.3
 */
public class SolrInputDocument extends SolrDocument
{
  private Map<String,Float> _boost = null;

  /**
   * Remove all fields and boosts from the document
   */
  @Override
  public void clear()
  {
    super.clear();
    if( _boost != null ) {
      _boost.clear();
    }
  }
  
  /**
   * Set the document boost.  null will remove the boost
   */
  public void setDocumentBoost( Float v )
  {
    this.setBoost( null, v );
  }
  
  /**
   * @return the document boost.  or null if not set
   */
  public Float getDocumentBoost()
  {
    return this.getBoost( null );
  }
  
  /**
   * Get the lucene document boost for a field.  Passing in <code>null</code> returns the
   * document boost, not a field boost.  
   */
  public void setBoost(String name, Float boost) {
    if( _boost == null ) {
      _boost = new HashMap<String, Float>();
    }
    if( boost == null ) {
      _boost.remove( name );
    }
    else {
      _boost.put( name, boost );
    }
  }

  /**
   * Set the field boost.  All fields with the name will have the same boost.  
   * Passing in <code>null</code> sets the document boost.
   * @param boost
   */
  public Float getBoost(String name) {
    if( _boost == null ) {
      return null;
    }
    return _boost.get( name );
  }
}

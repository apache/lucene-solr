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

/**
 * Represents a query item (search term). It consists of a fieldName and a value. .
 * 
 * @param properties A map of fields to set. Refer to the list of non-private fields.
 * @class QueryItem
 */
jQuery.solrjs.QueryItem = jQuery.solrjs.createClass (null, /** @lends jQuery.solrjs.QueryItem.prototype */  { 
    
  /** 
   * the field name.
   * @field 
   * @public
   */
  field : "",  
   
  /** 
   * The value
   * @field 
   * @public
   */
  value : "",  
    
  /**
   * creates a lucene query syntax, eg pet:"Cats"
   */  
  toSolrQuery: function() {
		return "(" + this.field + ":\"" + this.value + "\")";
  },
  
  /**
   * Uses fieldName and value to compare items.
   */
  equals: function(obj1, obj2) {
  	if (obj1.field == obj2.field && obj1.value == obj2.value) {
  		return true;
  	}
  	return false;
  }
    
});

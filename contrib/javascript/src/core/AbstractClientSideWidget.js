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
 * Baseclass for client side widgets. 
 *
 * <p> The json response writer is used, the widget gets the result object passed
 *     to the handleResult() method
 * </p>
 * 
 * @param properties A map of fields to set. Refer to the list of non-private fields. 
 * @class AbstractClientSideWidget
 * @augments jQuery.solrjs.AbstractWidget
 */
jQuery.solrjs.AbstractClientSideWidget = jQuery.solrjs.createClass ("AbstractWidget", /** @lends jQuery.solrjs.AbstractClientSideWidget.prototype */ { 
  
  /**
   * Adds the JSON specific request parameters to the url and creates a JSON call
   * using a dynamic script tag.
   * @param url The solr query request
   */
  executeHttpRequest : function(url) { 
  	url += "&wt=json&json.wrf=?&jsoncallback=?";  
    var me = this;
  	jQuery.getJSON(url,
  	  function(data){
  		me.handleResult(data);    
  	  }
  	);
  }, 
  
  /**
   * An abstract hook for child implementations. It is a callback that
   * is execute after the solr response data arrived.
   * @param data The solr response inside a javascript object.
   */
  handleResult : function(data) { 
	 throw "Abstract method handleResult"; 
  }

});

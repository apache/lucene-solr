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
 * Baseclass for server side widgets.
 * 
 * <p> The velocity response writer is used, the widget only specifies the
 *     template name in the getTemplateName() method. </p>
 *
 * @param properties A map of fields to set. Refer to the list of non-private fields. 
 * @class AbstractServerSideWidget
 * @augments jQuery.solrjs.AbstractWidget
 */
jQuery.solrjs.AbstractServerSideWidget = jQuery.solrjs.createClass ("AbstractWidget", /** @lends jQuery.solrjs.AbstractServerSideWidget.prototype */  { 
  
  /**
   * Adds the velocity specific request parameters to the url and creates a JSON call
   * using a dynamic script tag. The html response from the velocity template gets wrapped inside a 
   * javascript object to make cross site requests possible.
   * 
   * @param url The solr query request
   */
  executeHttpRequest : function(url) { 
    url += "&wt=velocity&v.response=QueryResponse&v.json=?&jsoncallback=?&v.contentType=text/json&v.template=" + this.getTemplateName();
    url += "&solrjs.widgetid=" + this.id;   
    var me = this;
    jQuery.getJSON(url,
      function(data){
        me.handleResult(data.result);
      }
    );
  },
  
  getTemplateName : function() { 
   throw("Abstract method");
  },
  
  /**
   * The default behaviour is that the result of the template is simply "copied" to the target div.
   * 
   * @param result The result of the velocity template wrapped inside a javascript object.
   */
  handleResult : function(result) { 
    jQuery(this.target).html(result);
  },

});


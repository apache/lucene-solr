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
 * A simple base class for result widgets (list of documents, including paging).
 * Implementations should override the renderResult(docs, pageSize, offset, numFound)
 * funtion to render the result.
 *
 * @class ExtensibleResultWidget
 * @augments jQuery.solrjs.AbstractClientSideWidget
 */
jQuery.solrjs.ExtensibleResultWidget = jQuery.solrjs.createClass ("AbstractClientSideWidget", { 
  
  isResult : true,
  
  getSolrUrl : function(start) { 
		return ""; // no special params need
	},

  handleResult : function(data) { 
    jQuery(this.target).empty();
    this.renderResult(data.response.docs, parseInt(data.responseHeader.params.rows), data.responseHeader.params.start, data.response.numFound);
	},

  renderResult : function(docs, pageSize, offset, numFound) { 
		throw "Abstract method renderDataItem";
  }
});
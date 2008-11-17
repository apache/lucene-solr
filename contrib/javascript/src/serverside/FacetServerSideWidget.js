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
 * Simple server side facet widget. Uses template "facets".
 * @class FacetServerSideWidget
 * @augments jQuery.solrjs.AbstractServerSideWidget
 */
jQuery.solrjs.FacetServerSideWidget = jQuery.solrjs.createClass ("AbstractServerSideWidget", /** @lends jQuery.solrjs.FacetServerSideWidget.prototype */ { 

  saveSelection : true,

  getSolrUrl : function(start) { 
    return "&facet=true&facet.field=" + this.fieldName;
  },

  getTemplateName : function() { 
  	return "facets"; 
  },  

  handleSelect : function(data) { 
	  jQuery(this.target).html(this.selectedItems[0].value);
    jQuery('<a/>').html("(x)").attr("href","javascript:solrjsManager.deselectItems('" + this.id + "')").appendTo(this.target);
  },

  handleDeselect : function(data) { 
    // do nothing, just refresh the view
  }	   

});
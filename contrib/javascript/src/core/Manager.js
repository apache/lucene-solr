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
 * The "Manager" acts as a container for all widgets. 
 *
 * <p> It stores solr configuration and selection and delegates calls to the widgets.
 *     All public calls should be performed on the manager object. </p>
 * <p> There has to be exactly one instance called "solrjsManager" present </p> 
 *     
 *
 * @example
 *  var solrjsManager;
 *   $sj(document).ready(function(){
 *       solrjsManager = new $sj.solrjs.Manager(
 *         { solrUrl:"http://localhost:8983/solr/select", 
 *           resourcesBase: "../../src/resources" });
 *   });
 *
 * @param properties A map of fields to set. Refer to the list of non-private fields.
 * @class Manager
 */
jQuery.solrjs.Manager = jQuery.solrjs.createClass (null,  /** @lends jQuery.solrjs.Manager.prototype */ {

  /** 
   * The absolute url to the solr instance
   * @field 
   * @default http://localhost:8983/solr/select/
   * @public
   */
   solrUrl : "http://localhost:8983/solr/select/",
   
  /** 
   * A path (absolute or relative) to the base directory of solrjs resources (css, imgs,..)
   * @field 
   * @public
   */
   resourcesBase : "",

  /** 
   * A constant representing "all documents"
   * @field 
   * @private 
   */
  QUERY_ALL : "*:*",  
  
  /** 
   * A collection of all registered widgets. For internal use only. 
   * @field 
   * @private 
   */
  widgets : [],
  
  /** 
   * A collection of the currently selected QueryItems. For internal use only. 
   * @field 
   * @private 
   */
  queryItems : [],
  
  /** 
   * A collection of the attached selection views. 
   * @field 
   * @private 
   */
  selectionViews : [],

 /** 
   * Adds a widget to this manager. 
   * @param {jQuery.solrjs.AbstractWidget} widget An instance of AbstractWidget. 
   */
  addWidget : function(widget) { 
		widget.manager = this;
 	  this.widgets[widget.id] = widget;
 	  widget.afterAdditionToManager();
	},
	
 /** 
   * Adds a selection view to this manager. 
   * @param {jQuery.solrjs.AbstractSelectionView} widget An instance of AbstractSelectionView. 
   */
  addSelectionView : function(view) { 
    view.manager = this;
    this.selectionViews[view.id] = view;
  },

  /** 
   * Adds the given items to the current selection.
   * @param widgetId The widgetId of where these items were selected. 
   * @param items A list of newly selected items. 
   */
	selectItems: function(widgetId, items){
  	this.widgets[widgetId].select(items);
  	var querySizeBefore = this.queryItems.length;
  	for (var i = 0; i < items.length; ++i) {
      if (!this.containsItem(items[i])) {
        this.queryItems.push(items[i]);
      }
    }
    if (querySizeBefore < this.queryItems.length) {
      this.doRequest(0);
    }
  },
  
  /** 
   * Removes the given items from the current selection.
   * @param widgetId The widgetId of where these items were deselected. 
   */  
  deselectItems: function(widgetId){
  	var widget = this.widgets[widgetId];
  	for (var i = 0; i < widget.selectedItems.length; i++) {
      for (var j = 0; j < this.queryItems.length; j++) {
        if (this.queryItems[j].toSolrQuery() ==  widget.selectedItems[i].toSolrQuery()) {
          this.queryItems.splice(j, 1);
        }   
      }
    }     
  	widget.deselect();
  	this.doRequest(0);
  },
  
  /** 
   * Removes the given item from the current selection, regardless of widgets.
   * @param widgetId The widgetId of where these items were deselected. 
   */  
  deselectItem: function(solrQuery){
    for (var j = 0; j < this.queryItems.length; j++) {
      var s = this.queryItems[j].toSolrQuery();
      if (s ==  solrQuery) {
        this.queryItems.splice(j, 1);
      }   
    }
    this.doRequest(0);
  },
    
  /** 
   * Checks if the given item is available in the current selection.
   * @param {jQuery.solrjs.QueryItem} item The item to check.
   */  
  containsItem: function(item){
  	for (var i = 0; i < this.queryItems.length; ++i) {
  		if (this.queryItems[i].toSolrQuery() == item.toSolrQuery()) {
  			return true;
  		}		
  	}
  	return false;
  },
  
  /** 
   * Creates a query out of the current selection and calls all bound widgets to
   * request their data from the server.
   * @param start The solr start offset parameter (mostly for result widgets).
   * @param resultsOnly Indicates that only the page changed and only result widgets should repaint).
   */ 
  doRequest : function(start, resultsOnly) { 
		var query = "";
    	
  	if (this.queryItems.length == 0) {
  		query = this.QUERY_ALL;
  	} else {
  		for (var i = 0; i < this.queryItems.length; ++i) {
  			query += this.queryItems[i].toSolrQuery();
  			if (i < this.queryItems.length -1) {
  				query += " AND ";
  			}
  		}
  	}

	  for (var id in this.widgets) {
	  	this.widgets[id].doRequest(query, start, resultsOnly);
	  }
	  
	  for (var id in this.selectionViews) {
      this.selectionViews[id].displaySelection(this.queryItems);
    }
	}, 

  /** 
   * Sets the current selection to *:* and requests all docs.
   */ 
	doRequestAll: function() {
    this.queryItems=[];
    this.doRequest(0);   
  },
  
  clearSelection: function() {
    this.queryItems=[];
  },
  
  /** 
   * Helper method that returns an ajax-loading.gif inside a div.
   */ 
  getLoadingDiv : function() {
    var div = jQuery("<div/>");
    jQuery("<img/>").attr("src", this.resourcesBase + "/img/ajax-loader.gif" ).appendTo(div);
    return div;
  }
});

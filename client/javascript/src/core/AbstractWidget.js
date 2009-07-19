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
 * Baseclass for all widgets. 
 * 
 * <p> Handles selection and request of items (called by the manager) and provides 
 *     abstract hooks for child classes to display the data.  </p>
 *
 * @param properties A map of fields to set. Refer to the list of non-private fields.  
 * @class AbstractWidget
 */
jQuery.solrjs.AbstractWidget = jQuery.solrjs.createClass (null, /** @lends jQuery.solrjs.AbstractWidget.prototype */ { 

  /** 
   * A unique identifier of this widget.
   *
   * @field 
   * @public
   */
  id : "", 

  /** 
   * The number of documents this widgets requests.
   * Normally only useful for result widgets.
   *
   * @field 
   * @default 0
   * @public
   */
	rows : 0, 
	
	/** 
   * A private flag that displays the selection status of a widget.
   * Selected widgets normally don't need to update their data when 
   * the selection changes.
   * 
   * @field 
   * @private
   */
	selected : false,
	
	/** 
   * A css classname representing the "target div" inside the html page.
   * All ui changes will be performed inside this empty div.
   * 
   * @field 
   * @private
   */
  target : "",
  
  /** 
   * A flag for result widgets. These widgets also get updated on page changes,
   * 
   * @field 
   * @private
   */
  isResult : false,
  
  /** 
   * A flag that indicates whether the loading icon should be displayed.
   * 
   * @field 
   * @private
   */
  showLoadingDiv : false,
  
  /** 
   * A flag that indicates whether the widget should save the current selection. If set to false,
   * a widget may be selected more than once.
   * 
   * @field 
   * @private
   */
  saveSelection : false,

  /**
   * Generates the solr request url for this widget and delegates the actual request
   * to the implementation. This method is only called by the manager.
   * 
   * @param query The current solr query
   * @start The offset.
   * @param resultsOnly Indicates that only the page changed and only result widgets should repaint).
   */
  doRequest : function(query, start, resultsOnly) { 
  	if (resultsOnly && !this.isResult) {
      return;
    }   
  	if (this.saveSelection && this.selected) {
			return;
		}    
		var solrRequestUrl = "";
		solrRequestUrl += this.manager.solrUrl;
		solrRequestUrl += "?";		
		solrRequestUrl += "&rows=" + this.rows;		
    solrRequestUrl += "&start=" + start;				
		solrRequestUrl += "&q=" + query;		
		solrRequestUrl += this.getSolrUrl(start);	

		// show loading gif
		if (this.showLoadingDiv) {
		  jQuery(this.target).html(this.manager.getLoadingDiv());
		}
		
		// let the implementation execute the call
		this.executeHttpRequest(solrRequestUrl);
		
	},

  /**
   * Marks this widget as selected and store selected items.
   * This method is only called by the manager.
   *
   * @param items The list of newly selected items.
   */
  select: function(items) {
  	if (this.saveSelection) {
    	this.selected = true;
  		this.selectedItems = items;
  	}
	  this.handleSelect();  	
  },
    
  /**
   * Marks this widget as unselected and clears the selected items.
   * This method is only called by the manager.
   */  
  deselect: function() {
    if (this.saveSelection) {
      this.selected = false;
    }  
    this.handleDeselect();
  },


	// Methods to be overridden by widgets:

  /**
   * An abstract hook for child implementations. This method should
   * execute the http request.
   * @param the complete solr request url.
   * @abstract 
   */
  executeHttpRequest : function(url) { 
		throw "Abstract method executeHttpRequest"; 
	},

  /** 
   * An abstract hook for child implementations. It should add widget specific
   * request parameters like facet=true..
   * @param start The offset.
   */
  getSolrUrl : function(start) { 
		// to be overridden
		return ""; 
	},

  /** 
   * An abstract hook for child implementations. Called after a widget is selected.
   * Child implementations should take care of changing the ui. 
   */
  handleSelect : function() { 
		// do nothing. Implementations may place some handler code here. 
	},

  /** 
   * An abstract hook for child implementations. Called after a widget is deselected.
   * Child implementations should take care of changing the ui. 
   */
	handleDeselect : function() { 
		// do nothing Implementations may place some handler code here. 
	},
	
	afterAdditionToManager : function() { 
    // do nothing by default. Implementations may place some init code here. 
  }
});

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
 * <p> Autocomplete input field that suggests facet values. It can show facet values of multiple 
 * fields (specified by "fieldNames"), as well as perform a fulltext query ("fulltextFieldName")
 * in case no suggested value is selected. </p>
 *
 * It uses the autocomplete box found at http://docs.jquery.com/UI/Autocomplete/autocomplete.
 * 
 * @class AutocompleteWidget
 * @augments jQuery.solrjs.AbstractClientSideWidget
 */
jQuery.solrjs.AutocompleteWidget = jQuery.solrjs.createClass ("AbstractClientSideWidget", /** @lends jQuery.solrjs.AutocompleteWidget.prototype */{ 

  /** 
   * A list of facet fields.
   * @field 
   * @public
   */
  fieldNames : [] ,  
  
  /** 
   * The field to search in if when no suggestion is selected.
   * @field 
   * @public
   */
  fulltextFieldName : "",  

  getSolrUrl : function(start) { 
    var ret = "&facet=true&facet.limit=-1";
    for (var j = 0; j < this.fieldNames.length; j++) {
      ret += "&facet.field=" + this.fieldNames[j];
    }
    return ret;
    
  },

  handleResult : function(data) { 
    
    // create new input field
    jQuery(this.target).empty();
    var input = jQuery('<input/>').attr("id", this.id + "_input").appendTo(this.target);
    
    // create autocomplete list
    var list = [];
    for (var j = 0; j < this.fieldNames.length; j++) {
      var field = this.fieldNames[j];
      var values = data.facet_counts.facet_fields[field];  
      for (var i = 0; i < values.length; i = i + 2) {
        var label = values[i] + " (" + values[i+1] + ") - " + field;      
        var value = values[i];
        list.push({text:label, value:value, field:field});
      }
    }
    
    // add selection listeners for suggests and fulltext search.
    var me = this;
    me.selectionMade = false;
    input.autocomplete(list, {
      formatItem: function(item) {
        return item.text;
      }
    }).result(function(event, item) {
      var items =  [new jQuery.solrjs.QueryItem({field: item.field , value:item.value})];
      solrjsManager.selectItems(me.id, items);
      me.selectionMade = true;
    });
    jQuery("#" + this.id + "_input").html("test").bind("keydown", function(event) {
      if (me.selectionMade == false && event.keyCode==13) {
        var items =  [new jQuery.solrjs.QueryItem({field: me.fulltextFieldName , value:"\"" + event.target.value + "\""})];
        solrjsManager.selectItems(me.id, items);
      }
    });
  }

});
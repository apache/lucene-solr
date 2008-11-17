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
 * Takes a solr field that stores an ISO-3166 country code. It creates facet values and
 * displays them in a selection dropdown as well as on a google chart map item. 
 * 
 * @class CountryCodeWidget
 * @augments jQuery.solrjs.AbstractClientSideWidget
 */
jQuery.solrjs.CountryCodeWidget = jQuery.solrjs.createClass ("AbstractClientSideWidget", { 

  /** 
   * The width of the map images.
   * 
   * @field 
   * @public
   */
  width : 350, 
  
  /** 
   * The height of the map images.
   *
   * @field 
   * @public
   */
  height : 180, 

  /** 
   * The field name of the iso country code field.
   *
   * @field 
   * @public
   */
  fieldName : "",  
  
  getSolrUrl : function(start) { 
		return "&facet=true&facet.mincount=1&facet.limit=-1&facet.field=" + this.fieldName;
  },

  handleResult : function(data) { 
	  jQuery(this.target).empty();
	  
	  // get facet counts
	  var values = data.facet_counts.facet_fields[this.fieldName];  
	  var maxCount = 0;
    var objectedItems = [];
    for (var i = 0; i < values.length; i = i + 2) {
      var c = parseInt(values[i+1]);
      if (c > maxCount) {
        maxCount = c;
      }
      objectedItems.push({label:values[i], count:values[i+1]});
    }
    
    // create a select for regions
    var container = jQuery("<div/>").attr("id",  "solrjs_" + this.id).appendTo(this.target);
    var select = jQuery("<select/>").appendTo(container);
    var me = this;
    select.change(function () {
      jQuery("#solrjs_" + me.id + " img").each(function (i,item) {
            jQuery(item).css("display", "none");
          });
      jQuery("#solrjs_" + me.id + this[this.selectedIndex].value).css("display", "block");
    });
    jQuery("<option/>").html("view the World ").attr("value", "world").appendTo(select);
    jQuery("<option/>").html("view Africa").attr("value", "africa").appendTo(select);
    jQuery("<option/>").html("view Asia").attr("value", "asia").appendTo(select);
    jQuery("<option/>").html("view Europe").attr("value", "europe").appendTo(select);
    jQuery("<option/>").html("view the Middle East").attr("value", "middle_east").appendTo(select);
    jQuery("<option/>").html("view South America").attr("value", "south_america").appendTo(select);
    jQuery("<option/>").html("view North America").attr("value", "usa").appendTo(select);
    
    // create a select for facet values
	  var codes = "";
	  var mapvalues = "t:";
	  var countrySelect = jQuery("<select/>").appendTo(container);
	  countrySelect.change(function () {
      var items =  [new jQuery.solrjs.QueryItem({field: me.fieldName , value:this[this.selectedIndex].value})];
      solrjsManager.selectItems(me.id, items);
    });
	  jQuery("<option/>").html("--select--").attr("value", "-1").appendTo(countrySelect);;
    
    // create map data
    for (var i = 0; i < objectedItems.length; i++) {
      if (objectedItems[i].label.length != 2) {
        continue;
      }
      codes += objectedItems[i].label;
      var currentValue = objectedItems[i].count;
      var percent =  (objectedItems[i].count / maxCount);
      var tagvalue = parseInt(percent * 100);       
      mapvalues += tagvalue + ".0";
      if (i < objectedItems.length - 1) {
        mapvalues += ",";
      }
      jQuery("<option/>").html(objectedItems[i].label + " (" + currentValue + ")").attr("value", objectedItems[i].label).appendTo(countrySelect);
    }
    
    // show maps
	  jQuery("<img/>").attr("id", "solrjs_" + this.id + "africa").css("display", "none").attr("src","http://chart.apis.google.com/chart?chco=f5f5f5,edf0d4,6c9642,365e24,13390a&chd=" + mapvalues + "&chf=bg,s,eaf7fe&chtm=africa&chld="+ codes +"&chs="+this.width+"x"+this.height+"&cht=t").appendTo(container);
	  jQuery("<img/>").attr("id", "solrjs_" + this.id + "asia").css("display", "none").attr("src","http://chart.apis.google.com/chart?chco=f5f5f5,edf0d4,6c9642,365e24,13390a&chd=" + mapvalues + "&chf=bg,s,eaf7fe&chtm=asia&chld="+ codes +"&chs="+this.width+"x"+this.height+"&cht=t").appendTo(container);
	  jQuery("<img/>").attr("id", "solrjs_" + this.id + "europe").css("display", "none").attr("src","http://chart.apis.google.com/chart?chco=f5f5f5,edf0d4,6c9642,365e24,13390a&chd=" + mapvalues + "&chf=bg,s,eaf7fe&chtm=europe&chld="+ codes +"&chs="+this.width+"x"+this.height+"&cht=t").appendTo(container);
	  jQuery("<img/>").attr("id", "solrjs_" + this.id + "middle_east").css("display", "none").attr("src","http://chart.apis.google.com/chart?chco=f5f5f5,edf0d4,6c9642,365e24,13390a&chd=" + mapvalues + "&chf=bg,s,eaf7fe&chtm=middle_east&chld="+ codes +"&chs="+this.width+"x"+this.height+"&cht=t").appendTo(container);
	  jQuery("<img/>").attr("id", "solrjs_" + this.id + "south_america").css("display", "none").attr("src","http://chart.apis.google.com/chart?chco=f5f5f5,edf0d4,6c9642,365e24,13390a&chd=" + mapvalues + "&chf=bg,s,eaf7fe&chtm=south_america&chld="+ codes +"&chs="+this.width+"x"+this.height+"&cht=t").appendTo(container);
	  jQuery("<img/>").attr("id", "solrjs_" + this.id + "usa").css("display", "none").attr("src","http://chart.apis.google.com/chart?chco=f5f5f5,edf0d4,6c9642,365e24,13390a&chd=" + mapvalues + "&chf=bg,s,eaf7fe&chtm=usa&chld="+ codes +"&chs="+this.width+"x"+this.height+"&cht=t").appendTo(container);
	  jQuery("<img/>").attr("id", "solrjs_" + this.id + "world").css("display", "block").attr("src","http://chart.apis.google.com/chart?chco=f5f5f5,edf0d4,6c9642,365e24,13390a&chd=" + mapvalues + "&chf=bg,s,eaf7fe&chtm=world&chld="+ codes +"&chs="+this.width+"x"+this.height+"&cht=t").appendTo(container);
	  
	  
	}
});
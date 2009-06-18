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
 * A calendar facet field. it uses solr's date facet capabilities, and displays 
 * the document count of one day using the DHTML calendar from www.dynarch.com/projects/calendar
 * 
 * @class CalendarWidget
 * @augments jQuery.solrjs.AbstractClientSideWidget
 */
jQuery.solrjs.CalendarWidget = jQuery.solrjs.createClass ("AbstractClientSideWidget", { 

  /** 
   * Start date, used to restrict the calendar ui as well 
   * as the solr date facets.
   *
   * @field 
   * @public
   */
  startDate : null,
  
  /** 
   * Start date, used to restrict the calendar ui as well 
   * as the solr date facets.
   *
   * @field 
   * @public
   */
  endDate : null,  
  
  /** 
   * Current date facet array.
   *
   * @field 
   * @private
   */
  dates : null,  
  
  getSolrUrl : function(start) { 
    return "&facet=true&facet.mincount=1&facet.date=date&facet.date.start=1987-01-01T00:00:00.000Z/DAY&facet.date.end=1987-11-31T00:00:00.000Z/DAY%2B1DAY&facet.date.gap=%2B1DAY";
  },

  handleResult : function(data) { 
  
    var me = this;
    me.dates = [];
    jQuery.each(data.facet_counts.facet_dates.date, function(key, value) {
      var date = new Date(key.slice(0, 4), parseInt(key.slice(6, 8)) - 1, key.slice(8, 10));
      me.dates[date] = value;
    });
    
    jQuery(this.target).empty();
    
    var parent = document.getElementById("calendar");

    // construct a calendar giving only the "selected" handler.
    var cal = new Calendar(0, null, function (cal, date) {
      if (cal.dateClicked) {
        var dateString = "[" + date + "T00:00:00Z TO " + date + "T23:59:59Z]";
        var items =  [new jQuery.solrjs.QueryItem({field: me.fieldName , value:date, toSolrQuery: function() { return "date:" + dateString }})];
        solrjsManager.selectItems(me.id, items);
      }
    });
    cal.dateClicked = false;
    cal.weekNumbers = false;
    cal.setDateFormat("%Y-%m-%d");
    cal.setTtDateFormat("solrjs");
    
    cal.setDateStatusHandler(function(date) { 
        if (me.dates[date] != null && me.dates[date] > 0) {
          return "solrjs solrjs_value_" + me.dates[date];
        }
        return true;
      });
      
    cal.create(parent);
    cal.show();
    cal.setDate(new Date(1987, 2, 1));
    
    // override print method to display document count
    var oldPrint = Date.prototype.print;
    Date.prototype.print = function(string) {
      if (string.indexOf("solrjs") == -1) {
        return oldPrint.call(this, string);
      }
      return me.dates[this] + " documents found!";
    }
  }

});


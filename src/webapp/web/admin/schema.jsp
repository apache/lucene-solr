<%@ page contentType="text/html; charset=utf-8" pageEncoding="UTF-8"%>
<%--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
--%>

<%-- $Id: index.jsp 608150 2008-01-02 17:15:30Z ryan $ --%>
<%-- $Source: /cvs/main/searching/SolrServer/resources/admin/index.jsp,v $ --%>
<%-- $Name:  $ --%>
  
<script src="jquery-1.2.3.min.js"></script>
<script>

(function($, libName) {
  var solr = {
    
    //The default location of the luke handler relative to this page
    // Can be overridden in the init(url) method
    pathToLukeHandler: 'luke',  
    
    // Base properties to hold schema information
    schemaInfo: {},
    schemaFields: {},
    schemaDynamicFields: {},
    schemaTypes: {},
    schemaFlags: {},
    
    //The basic function to call to make the initail JSON calls
    // takes one option parameter, the path to the luke handler
    // if undefined, it will use the default, 'luke', which means
    // this is being called from the same relative URL path
    init: function(pathToLukeHandler) {
      if (pathToLukeHandler != undefined) {
        solr.pathToLukeHandler = pathToLukeHandler;
      }
      solr.loadSchema(function() {
        solr.loadFromLukeHandler(function () {
          solr.createMenu('menu');
          solr.displaySchemaInfo();
        });
      });

    },
    
    //load the Schema from the LukeRequestHandler
    // this loads every field, and in each field the copy source/dests and flags
    // we also load the list of field types, and the list of flags
    loadSchema: function(func) {
			$.getJSON(solr.pathToLukeHandler +'?show=schema&wt=json', function(data) {
        //populate all non field/type/flag data in the info block
        $.each(data.index, function(i, item) {
          solr.schemaInfo[i] = item;
        });
        
        //LukeRequestHandler places these two attributes outside of the "index" node, but
        // we want it here so we can more easily display it in the "HOME" block
        solr.schemaInfo['uniqueKeyField'] = data.schema.uniqueKeyField;
        solr.schemaInfo['defaultSearchField'] = data.schema.defaultSearchField;
        //a one-off hack, because the directory string is so long and unbroken
        // that it can break CSS layouts
        solr.schemaInfo['directory'] = solr.schemaInfo['directory'].substring(0, solr.schemaInfo['directory'].indexOf('@')+1) + ' ' +  solr.schemaInfo['directory'].substring(solr.schemaInfo['directory'].indexOf('@') +1);
        
        // populate the list of fields
				$.each(data.schema.fields, function(i,item){
					solr.schemaFields[i]=item;
     		});
        // populate the list of field types
	      $.each(data.schema.types, function(type, ft) {
          solr.schemaTypes[type] = ft;
        });
        //populate the list of dynamic fields
        $.each(data.schema.dynamicFields, function(i, dynField) {
          solr.schemaDynamicFields[i] = dynField;
        });
        //populate the list of flags, so we can convert flags to text in display
      	$.each(data.info.key, function(i, flag) {
      		solr.schemaFlags[i] = flag;
 	    	});
        
        //LukeRequestHandler returns copyFields src/dest as the entire toString of the field
        // we only need the field name, so here we loop through the fields, and replace the full
        // field definitions with the name in the copySources/copyDests properties
        $.each(solr.schemaFields, function(i, field) {
          $.each(['copySources', 'copyDests'], function(i, copyProp) {
            var newFields = new Array();
            $.each(field[copyProp], function(i, fullName) {
              newFields.push(fullName.substring(fullName.lastIndexOf(':')+1, fullName.indexOf('{')));
            });
            field[copyProp] = newFields;
          });
        
        });
        //An additional optional callback
        // used in init to trigger the 2nd call to LukeRequestHandler only
        // after the first one is finished
        if ($.isFunction(func)) {
          func(solr);
        }
      });
    },

    //further populates the loaded schema with information gathered
    // from the no argument LukeRequestHandler
    loadFromLukeHandler: function(func) {
      $.getJSON(solr.pathToLukeHandler+'?wt=json', function(data) {
        $.each(data.fields, function(i, item) {
          var field = solr.schemaFields[i];
          
          //If undefined, then we have a dynamicField which does not show up
          // in the LukeRequestHandler show=schema variant
          if (field == undefined) {
            field = item;
            //Attach this field to its dynamicField
            var base = field.dynamicBase;
            var dynField = solr.schemaDynamicFields[base];

            //Some fields in a multicore setting have no dynamic base, either
            // the name of the core is a field that has no type or flags
            if (dynField != undefined) {
            	var synFields = dynField['fields'];
	            if (synFields== undefined) {
    	          synFields= new Array();
        	    }
            	synFields.push(i);
            	dynField['fields'] = synFields;
            }
            solr.schemaFields[i] = item;
          }
          //Populate other data in this field that would not have been loaded in
          // the show=schema variant
          $.each(item, function(k, v) {
            if (k == 'topTerms' || k == 'histogram') {
              solr.schemaFields[i][k] = solr.lukeArrayToHash(v);
            } else {
              solr.schemaFields[i][k] = v;
            }
          });
        });
        //another optional callback; used in the init case to lay out the page
        // after the data is loaded
        if ($.isFunction(func)) {
          func();
        }
      });
    },
    //some elements in the JSON response are arrays, where odd/even elements
    // are the name/value, and convert it to a standard map/associative array
    // incoming: ['foo', 'bar', 'bat', 'baz']
    // output: {'foo':'bar', 'bat':baz'}
    lukeArrayToHash: function(termsArr) {
        var hash = new Object();
				var temp;
        //topTerms comes in as an array, with odd indexes the field name
        // and even indexes the number
				$.each(termsArr, function(i, item) {
					if (i%2 ==0) {
						temp = item;
					} else {
						hash[temp] = item;
					} 
				});
				return hash;
    },
    
    //gets the top Terms via an Ajax call the LukeRequestHandler for that field
    // The callback is used here to redraw the table after the ajax call returns
		getTopTerms: function(fieldName, numTerms, func) {
      if (numTerms == undefined) {
        var numTerms = 10;
      }
      if (isNaN(numTerms) || numTerms <=0 || numTerms.indexOf('.') != -1) {
        return;
      }
			$.getJSON(solr.pathToLukeHandler+'?fl='+fieldName+'&wt=json&numTerms='+numTerms, function(data) {                  
				solr.schemaFields[fieldName]['topTerms'] = solr.lukeArrayToHash(data.fields[fieldName].topTerms);
        if ($.isFunction(func)) {
          func(solr.schemaFields[fieldName]['topTerms'], fieldName);
        }
			});
		},
    
    // Displays the SchemaInfo in the main content panel
    // dispayed on data load, and also when 'Home' is clicked
    displaySchemaInfo: function() {
      $('#mainInfo').html('');
      $('#topTerms').html('');
      $('#histogram').html('');      
      $('#mainInfo').append(solr.createSimpleText('Schema Information'));
      //Make sure the uniqueKeyField and defaultSearchFields come first
      $.each({'Unique Key':'uniqueKeyField', 'Default Search Field':'defaultSearchField'}, function(text, prop) {
          if (solr.schemaInfo[prop] != undefined) {
            $('#mainInfo').append(solr.createNameValueText(text, function(p) {
              p.appendChild(solr.createLink(solr.schemaInfo[prop], solr.schemaInfo[prop]));
              return p;
            }));
          } 
      });
      $.each(solr.schemaInfo, function(i, item) {
        if (i == 'uniqueKeyField' || i == 'defaultSearchField') {
          //noop; we took care of this above
        } else {
          $('#mainInfo').append(solr.createNameValueText(i, item));
        }
      });
      //Close all menus when we display schema home
      solr.toggleMenus(undefined, ['fields', 'types', 'dynFields']);
    },
    
    // display a dynamic field in the main content panel
    displayDynamicField: function(dynamicPattern) {
      var df = solr.schemaDynamicFields[dynamicPattern];
      $('#mainInfo').html('');
      $('#topTerms').html('');
      $('#histogram').html('');
      $('#mainInfo').append(solr.createSimpleText('Dynamic Field: ' + dynamicPattern));
      $('#mainInfo').append(solr.createNameValueText('Fields', function(p) {
        if (df.fields != undefined) {
          $.each(df.fields, function(i, item) {
            p.appendChild(solr.createLink(item, item));
          });
        } else {
          p.appendChild(document.createTextNode(' None currently in index'));
        }
        return p;
      }));
      var ft = solr.schemaTypes[df.type];
      $('#mainInfo').append(solr.createNameValueText('Field Type', function(p) {
        p.appendChild(solr.createLink(df.type, df.type, solr.displayFieldType));
        return p;
      }));
      if (df.flags != undefined) {
        $('#mainInfo').append(solr.createNameValueText('Properties', solr.createTextFromFlags(df.flags, df.type)));
      }
      solr.displayAnalyzer(ft.indexAnalyzer, 'Index Analyzer', true);
      solr.displayAnalyzer(ft.queryAnalyzer, 'Query Analyzer', true);

      solr.toggleMenus('dynFields', ['fields', 'types'], dynamicPattern);
    },
    
    // display a field type in the main area
    displayFieldType: function(typeName) {
      var ft = solr.schemaTypes[typeName];
      $('#mainInfo').html('');
      $('#topTerms').html('');
      $('#histogram').html('');
			$('#mainInfo').append(solr.createSimpleText('Field Type: ' + typeName));
        $('#mainInfo').append(solr.createNameValueText('Fields', function(p) {
          if (ft.fields != undefined) {
            $.each(ft.fields, function(i, item) {
              if (solr.schemaFields[item] != undefined) {
                p.appendChild(solr.createLink(item, item));
              } else {
                p.appendChild(solr.createLink(item, item, solr.displayDynamicField));
              }
              p.appendChild(document.createTextNode(' '));
            });
          } else {
            p.appendChild(document.createTextNode('No fields in index'));
          }
          return p;
        }));
      $('#mainInfo').append(solr.createNameValueText('Tokenized', ft.tokenized));
      $('#mainInfo').append(solr.createNameValueText('Class Name', ft.className));

      solr.displayAnalyzer(ft.indexAnalyzer, 'Index Analyzer');
      solr.displayAnalyzer(ft.queryAnalyzer, 'Query Analyzer');
      solr.toggleMenus('types', ['fields', 'dynFields'], typeName);
    },
    
    //Displays information about an Analyzer in the main content area
    displayAnalyzer: function(analyzer, type, shouldCollapse) {
      var tid = type.replace(' ', '');
      var collapse = shouldCollapse && (analyzer.tokenizer != undefined || analyzer.filters != undefined);
      $('#mainInfo').append(solr.createNameValueText(type, function(p) {
        p.appendChild(document.createTextNode(analyzer.className + ' '));
        if (collapse) {
          p.appendChild(solr.createLink(type, 'Details', function() {
            $('#'+tid).toggle("slow");
          }));
        }
        return p;
      }));
      var adiv = document.createElement('div');
      adiv.id=tid;
      adiv.className='analyzer';
      if (collapse) {
        adiv.style.display='none';
      }
      if (analyzer.tokenizer != undefined) {
        adiv.appendChild(solr.createNameValueText("Tokenizer Class", analyzer.tokenizer.className));
      }
      if (analyzer.filters != undefined) {
        adiv.appendChild(solr.createNameValueText('Filters', ''));
        var f = document.createElement('ol');
        $.each(analyzer.filters, function(i, item) {
          var fil = document.createElement('li');
          var filterText = item.className;
          if (item.args != undefined) {
            filterText += ' args:{'
            $.each(item.args, function(fi, fitem) {
              filterText += fi + ': ' + fitem + ' ';
            });
            filterText +='}';
            fil.innerHTML = filterText;
            f.appendChild(fil);
          }
        });
        adiv.appendChild(f);
      }
      $('#mainInfo').append(adiv);
    },
    
    // display information about a Field in the main content area
    // and its TopTerms and Histogram in related divs
		displayField: function(fieldName) {
      var field = solr.schemaFields[fieldName];
      var isDynamic = field.dynamicBase != undefined ? true : false;
      var ft;
      var ftName;
      $('#mainInfo').html('');  
      $('#topTerms').html('');
      $('#histogram').html('');
      $('#mainInfo').append(solr.createSimpleText('Field: ' + fieldName));
      
      //For regular fields, we take their properties; for dynamicFields,
      // we take them from their dynamicField definitions
      if (isDynamic) {
        ftName = solr.schemaDynamicFields[field.dynamicBase].type
        $('#mainInfo').append(solr.createNameValueText('Dynamically Created From Pattern', function(p) {
          p.appendChild(solr.createLink(field.dynamicBase, field.dynamicBase, solr.displayDynamicField));
          return p;
        }));
      } else {
        ftName = field.type;
      }			
      ft = solr.schemaTypes[field.type];
      $('#mainInfo').append(solr.createNameValueText('Field Type', function(p) {
        p.appendChild(solr.createLink(ftName, ftName, solr.displayFieldType));
        return p;
      }));
			if (solr.schemaFlags != '') {
        $.each({'flags':'Properties', 'schema':'Schema', 'index':'Index'}, function(prop, text) {
          if (field[prop] != undefined) {
            $('#mainInfo').append(solr.createNameValueText(text, solr.createTextFromFlags(field[prop], ft)));
          }
        });
      }    
      $.each({'copySources':'Copied From', 'copyDests':'Copied Into'}, function(prop, text) {
        if (field[prop] != undefined && field[prop] != '') {
          $('#mainInfo').append(solr.createNameValueText(text, function(p) {
            $.each(field[prop], function(i, item) {
              p.appendChild(solr.createLink(item, item));
              p.appendChild(document.createTextNode(' '));
            });
            return p;
          }));
        }
      });
      if (field.positionIncrementGap != undefined) {
        $('#mainInfo').append(solr.createNameValueText('Position Increment Gap', field.positionIncrementGap));
      }
      solr.displayAnalyzer(ft.indexAnalyzer, 'Index Analyzer', true);
      solr.displayAnalyzer(ft.queryAnalyzer, 'Query Analyzer', true);
      if (field.docs != undefined) {
        $('#mainInfo').append(solr.createNameValueText('Docs', field.docs));
      }
      if (field.distinct != undefined) {
        $('#mainInfo').append(solr.createNameValueText('Distinct', field.distinct));
      }

      if (field.topTerms != undefined) {
        solr.displayTopTerms(field.topTerms, fieldName);
      }

      if (field.histogram != undefined) {
        solr.drawHistogram(field.histogram);
      }
      solr.toggleMenus('fields', ['types', 'dynFields'], fieldName);
		},	

    //utility method to create a single sentence list of properties from a flag set
    // or pass it on, if the flags are (unstored field)
		createTextFromFlags: function(fieldFlags, fieldType) {
			var value;
      if (fieldFlags != '(unstored field)') {
        var value = '';      
        for (var i=0;i<fieldFlags.length;i++) {
          if (fieldFlags.charAt(i) != '-') {
            value += solr.schemaFlags[fieldFlags.charAt(i)];
          value += ', ';
          }
        }
        value = value.substring(0, value.length-2);
			} else {
      value = fieldFlags;
      }
			return value;
		},

    //Store the currently highlighted menu item, as otherwise we
    // must traverse all li menu items, which is very slow on schemas with
    // large number of fields
    // for example $('#menu ul li').siblings().removeClass('selected');
    currentlyHighlightedMenuId: undefined,
    
    //add a highlight to the currently selected menu item, and remove
    // the highlights from all other menu items
    highlightMenuItem: function(idToSelect) {
      if (solr.currentlyHighlightedMenuId != undefined) {
        $('#'+solr.currentlyHighlightedMenuId).removeClass('selected');
      }
      $('#'+idToSelect).addClass('selected');
      solr.currentlyHighlightedMenuId = idToSelect;
    },
    
    //Opens one menu group, close the others, and optionally highlight one
    // item, which should be in the opened menu
    toggleMenus: function(idToShow, idsToHide, idToSelect) {
      if (idToSelect != undefined) {
        solr.highlightMenuItem(idToShow + idToSelect);
      }
      $('#'+idToShow).show("slow");
      $.each(idsToHide, function(i, idToHide) {
        $('#'+idToHide).hide("slow");
      });
    },
    
    //A utility method to create a paragraph, which takes two arguments;
    // an opening text, and either text or a callback function to follow
    // any callback function must return the node passed into it
    createNameValueText: function(openingText, func) {
      var p = document.createElement('p');
      p.appendChild(solr.createSimpleText(openingText + ': ', 'b'));
      return solr.applyFuncToNode(p, func);
    },

    //utility method to create an HTML text element node
    // with the literal text to place, and an optional function to apply
    // any callback function must return the node passed into it 
    createSimpleText: function(text, n, func) {
      if (n == undefined) {
        n = 'h2';
      }
      var no= document.createElement(n);
      no.appendChild(document.createTextNode(text));
      return solr.applyFuncToNode(no, func);
    },
    
    //Utility method that applies a function or a string to append
    // an additional child to a node
    applyFuncToNode: function(no, func) {
      if ($.isFunction(func)) {
        no = func(no);
      } else {
        // if it is not a function, append it as a string
        if (func != undefined) {
          no.appendChild(document.createTextNode(' ' + func));
        }
      }
      return no;
    },
        
    //show a table of top terms for a given field
    displayTopTerms: function(topTerms, fieldName) {
        $('#topTerms').html('');
        var tbl = document.createElement('table');
        tbl.className='topTerms';
        var thead= document.createElement('thead');
        var headerRow = document.createElement('tr');
        $.each(['term', 'frequency'], function() {
          var cell = document.createElement('th');
          cell.innerHTML= this;
          headerRow.appendChild(cell);
        });
        thead.appendChild(headerRow);
        tbl.appendChild(thead);
        var tbody = document.createElement('tbody');
        
        var numTerms = 0;
        $.each(topTerms, function(term, count) {
          var c1 = $('<td>').text(term);
          var c2 = $('<td>').text(count);
          var row = $('<tr>').append(c1).append(c2);
          tbody.appendChild(row.get(0));
          numTerms++;
        });
        tbl.appendChild(tbody);
        
        //create a header along with an input widget so the user
        // can request a different number of Top Terms
        var h2 = document.createElement('h2');
        h2.appendChild(document.createTextNode('Top   '));
        var termsGetter = document.createElement('input');
        termsGetter.type='text';
        termsGetter.size=5;
        termsGetter.value=numTerms;
        
        termsGetter.onchange=function() {
            solr.getTopTerms(fieldName, this.value, solr.displayTopTerms);
        }
        h2.appendChild(termsGetter);
        h2.appendChild(document.createTextNode(' Terms'));
        $('#topTerms').append(h2);
        
        document.getElementById('topTerms').appendChild(tbl);
        $('#topTerms').append(tbl);
    },
    
    //draws a histogram, taking a map of values and an optional total height and width for the table
    drawHistogram: function(histogram, totalHeightArg, totalWidthArg) {
      $('#histogram').html('');
      $('#histogram').append(solr.createSimpleText('Histogram'));
      var max = 0;
      var bars =0;
      //find the # of columns and max value in the histogram 
      // so we can create an appropriately scaled chart
      $.each(histogram, function(i, item) {
        if (item > max) max = item;
        bars += 1;
      });
      if (max ==0) {
        $('#histogram').append(solr.createNameValueText('No histogram available'));
      } else {
        var totalHeight = totalHeightArg == undefined ? 208 : totalHeightArg;
        var totalWidth = totalWidthArg == undefined ? 160 : totalWidthArg;
        var tbl = document.createElement('table');
        tbl.style.width=totalWidth+'px';
        tbl.className = 'histogram';
        var h = document.createElement('tbody');
        var r = document.createElement('tr');
        var r2 = document.createElement('tr');
        $.each(histogram, function(i, item) {
          var c = document.createElement('td');
          c.innerHTML=item+'<div style="width:'+totalWidth/bars+'px;height:'+(item*totalHeight/max)+'px;background:blue">&nbsp</div>';
          r.appendChild(c);
          var c2 = document.createElement('td');
          c2.innerHTML='' + i;
          r2.appendChild(c2);
        });
        h.appendChild(r);
        h.appendChild(r2);
        tbl.appendChild(h);
        $('#histogram').append(tbl);
      }
    },
    
    //dynamically creates a link to be appended
    createLink: function(idToDisplay, linkText, linkFunction) {
      var link = document.createElement('a');
      if (!$.isFunction(linkFunction)) {
        linkFunction = solr.displayField
      }
      link.onclick=function() {
        linkFunction(idToDisplay);
        return false;
      };
      link.href='#';
      link.innerHTML=linkText;
      return link;
    },
    
    //Creates a menu header that can expand or collapse its children
    createMenuHeader: function(text, idToShow, idsToHide) {
      var head = document.createElement('h3');
      var a = document.createElement('a');
      a.onclick=function() {
        solr.toggleMenus(idToShow, idsToHide);
        return false;
      };
      a.href='#';
      a.innerHTML=text;
      head.appendChild(a);
      return head;
    },
    
    //Creates an element in a menu (e.g. each field in a list of fields)
    createMenuItem: function(tagName, text, link, type, func) {
        var fieldEle = document.createElement('li');
        fieldEle.id=type+text;
        var funct = func == undefined ? undefined : func;
        fieldEle.appendChild(solr.createLink(text, link, funct));
        return fieldEle;
    },
    
    //populates the menu div
    createMenu: function(menuId) {
      var m = $('#'+menuId);
      var home = document.createElement('h2');
      home.appendChild(solr.createLink('Home', 'Home', solr.displaySchemaInfo));
      m.append(home);
      m.append(solr.createMenuHeader('Fields', 'fields', ['types', 'dynFields']));
      var fields= document.createElement('ul');
      fields.style.display='none';
      fields.id = 'fields';
      $.each(solr.schemaFields, function(i, item) {
        fields.appendChild(solr.createMenuItem('li', i, i, fields.id));
      });
      m.append(fields);
      m.append(solr.createMenuHeader('Dynamic Fields', 'dynFields', ['fields', 'types']));
      var dyns = document.createElement('ul');
      dyns.style.display = 'none';
      dyns.id = 'dynFields';
      $.each(solr.schemaDynamicFields, function(i, item) {
        dyns.appendChild(solr.createMenuItem('li', i,i, dyns.id, solr.displayDynamicField));
      });
      m.append(dyns);
      m.append(solr.createMenuHeader('Field Types', 'types', ['fields', 'dynFields']));
      var types = document.createElement('ul');
      types.style.display='none';
      types.id='types';
      $.each(this.schemaTypes, function(i, item) {
        types.appendChild(solr.createMenuItem('li', i, i,types.id, solr.displayFieldType));
      });
      m.append(types);
    }
   };
   
	window[libName] = solr;
})(jQuery, 'solr');
$(document).ready(function() {
  solr.init();
});
    
$(window).unload( function() {
  solr = null;
  $('#mainInfo').html('');
  $('#menu').html('');
  $('#topTerms').html('');
  $('#histogram').html('');
});
  
</script>
<%-- do a verbatim include so we can use the local vars --%>
<%@include file="header.jsp" %>
<div id="schemaTop">
<h2>Schema Browser | See <a href="file/?file=schema.xml">Raw Schema.xml</a></h2>
</div>
<div id="menu"></div>
<div id="content">
<div id="mainInfo"><h2>Please wait...loading and parsing Schema Information from LukeRequestHandler</h2><p>If it does not load or your browser is not javascript or ajax-capable, you may wish to examine your schema using the <a href="luke?wt=xslt&tr=luke.xsl">Server side transformed LukeRequestHandler</a> or the raw <a href="file/?file=schema.xml">schema.xml</a> instead.</div>
<div id="topTerms"></div>
<div id="histogram"></div>
</div>
</body>
</html>

/*
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
*/

solrAdminApp.controller('AnalysisController',
  function($scope, $location, $routeParams, Luke, Analysis) {
      $scope.resetMenu("analysis");

      $scope.refresh = function() {
        Luke.schema({core: $routeParams.core}, function(data) {
          $scope.fieldsAndTypes = [];
          for (var field in data.schema.fields) {
            $scope.fieldsAndTypes.push({
              group: "Fields",
              value: "fieldname=" + field,
              label: field});
          }
          for (var type in data.schema.types) {
            $scope.fieldsAndTypes.push({
              group: "Types",
              value: "fieldtype=" + type,
              label: type});
          }
        });

        $scope.parseQueryString();
        // @todo - if URL param, set $scope.verbose;
        // @todo - set defaultSearchField either to context["analysis.fieldname"] or context["analysis.fieldtype"]

      };
      $scope.verbose = true;

      var getShortComponentName = function(longname) {
        var short = -1 !== longname.indexOf( '$' )
                         ? longname.split( '$' )[1]
                         : longname.split( '.' ).pop();
        return short.match( /[A-Z]/g ).join( '' );
      };

      var getCaptionsForComponent = function(data) {
        var captions = [];
        for (var key in data[0]) {
          key = key.replace(/.*#/,'');
          if (key != "match" && key!="positionHistory") {
            captions.push(key.replace(/.*#/,''));
          }
        }
        return captions;
      };

      var getTokensForComponent = function(data) {
        var tokens = [];
        var previousPosition = 0;
        var index=0;
        for (var i in data) {
          var tokenhash = data[i];
          var positionDifference = tokenhash.position - previousPosition;
          for (var j=positionDifference; j>1; j--) {
            tokens.push({position: tokenhash.position - j+1, blank:true, index:index++});
          }

          var token = {position: tokenhash.position, keys:[], index:index++};

          for (key in tokenhash) {
            if (key == "match" || key=="positionHistory") {
              //@ todo do something
            } else {
              token.keys.push({name:key, value:tokenhash[key]});
            }
          }
          tokens.push(token);
          previousPosition = tokenhash.position;
        }
        return tokens;
      };

      var extractComponents = function(data, result, name) {
        if (data) {
            result[name] = [];
            for (var i = 0; i < data.length; i += 2) {
                var component = {
                    name: data[i],
                    short: getShortComponentName(data[i]),
                    captions: getCaptionsForComponent(data[i + 1]),
                    tokens: getTokensForComponent(data[i + 1])
                };
                result[name].push(component);
            }
        }
      };

      var processAnalysisData = function(analysis, fieldOrType) {
        var fieldname;
        for (fieldname in analysis[fieldOrType]) {console.log(fieldname);break;}
        var response = {};
        extractComponents(analysis[fieldOrType][fieldname].index, response, "index");
        extractComponents(analysis[fieldOrType][fieldname].query, response, "query");
        return response;
      };

      $scope.updateQueryString = function() {

        var parts = $scope.fieldOrType.split("=");
        var fieldOrType = parts[0];
        var name = parts[1];

        if ($scope.indexText) {
            $location.search("analysis.fieldvalue", $scope.indexText);
        }
        if ($scope.queryText) {
          $location.search("analysis.query", $scope.queryText);
        }

        if (fieldOrType == "fieldname") {
          $location.search("analysis.fieldname", name);
          $location.search("analysis.fieldtype", null);
        } else {
          $location.search("analysis.fieldtype", name);
          $location.search("analysis.fieldname", null);
        }
      };

      $scope.parseQueryString = function () {
          var params = {};
          var search = $location.search();

          if (Object.keys(search).length == 0) {
              return;
          }
          for (var key in search) {
              params[key]=search[key];
          }
          $scope.indexText = search["analysis.fieldvalue"];
          $scope.queryText = search["analysis.query"];
          if (search["analysis.fieldname"]) {
              $scope.fieldOrType = "fieldname=" + search["analysis.fieldname"];
          } else {
              $scope.fieldOrType = "fieldtype=" + search["analysis.fieldtype"];
          }
          $scope.verbose = search["verbose_output"] == "1";

          if ($scope.fieldOrType || $scope.indexText || $scope.queryText) {
            params.core = $routeParams.core;
            var parts = $scope.fieldOrType.split("=");
            var fieldOrType = parts[0] == "fieldname" ? "field_names" : "field_types";

              Analysis.field(params, function(data) {
              $scope.result = processAnalysisData(data.analysis, fieldOrType);
            });
          }
      };


      $scope.toggleVerbose = function() {$scope.verbose = !$scope.verbose};

      $scope.refresh();
    }
);

/***************
// #/:core/analysis
sammy.get
(
        var analysis_element = $( '#analysis', content_element );
        var analysis_form = $( 'form', analysis_element );
        var analysis_result = $( '#analysis-result', analysis_element );
        analysis_result.hide();

        var verbose_link = $( '.verbose_output a', analysis_element );

        var type_or_name = $( '#type_or_name', analysis_form );
        var schema_browser_element = $( '#tor_schema' );
        var schema_browser_path = app.core_menu.find( '.schema-browser a' ).attr( 'href' );
        var schema_browser_map = { 'fieldname' : 'field', 'fieldtype' : 'type' };

        type_or_name
          .die( 'change' )
          .live
          (
            'change',
            function( event )
            {
              var info = $( this ).val().split( '=' );

              schema_browser_element
                .attr( 'href', schema_browser_path + '?' + schema_browser_map[info[0]] + '=' + info[1] );
            }
          );

========================

        $( '.analysis-error .head a', analysis_element )
          .die( 'click' )
          .live
          (
            'click',
            function( event )
            {
              $( this ).parents( '.analysis-error' )
                .toggleClass( 'expanded' );
            }
          );

========================

        var check_empty_spacer = function()
        {
          var spacer_holder = $( 'td.part.data.spacer .holder', analysis_result );

          if( 0 === spacer_holder.size() )
          {
            return false;
          }

          var verbose_output = analysis_result.hasClass( 'verbose_output' );

          spacer_holder
            .each
            (
              function( index, element )
              {
                element = $( element );

                if( verbose_output )
                {
                  var cell = element.parent();
                  element.height( cell.height() );
                }
                else
                {
                  element.removeAttr( 'style' );
                }
              }
            );
        }
========================

        verbose_link
          .die( 'toggle' )
          .live
          (
            'toggle',
            function( event, state )
            {
              $( this ).parent()
                .toggleClass( 'active', state );

              analysis_result
                .toggleClass( 'verbose_output', state );

              check_empty_spacer();
            }
          )
          .die( 'click' )
          .live
          (
            'click',
            function( event )
            {
              $( this ).parent()
                .toggleClass( 'active' );

              analysis_form.trigger( 'submit' );
            }
          );


========================
        analysis_form
          .die( 'submit' )
          .live
          (
            'submit',
            function( event )
            {
           var params = analysis_form.formToArray();
/****
var params = {
"analysis.fieldvalue": $scope.indexText,
"analysis.query": $scope.queryText,
"analysis.fieldname": $scope.field,
"verbose_output": $scope.verbose ? 1:0};

              var params = $.param( compute_analysis_params() )
                            .replace( /[\w\.]+=\+*(&)/g, '$1' ) // remove empty parameters
                            .replace( /(&)+/, '$1' )            // reduce multiple ampersands
                            .replace( /^&/, '' )                // remove leading ampersand
                            .replace( /\+/g, '%20' );           // replace plus-signs with encoded whitespaces

$location.path($params);
Analysis.fields({}, function(data) {

              var url = core_basepath + '/analysis/field?wt=json&analysis.showmatch=true&' + context.path.split( '?' ).pop();
              url = url.replace( /&verbose_output=\d/, '' );

                    for( var name in response.analysis.field_names )
                    {
                      build_analysis_table( 'name', name, response.analysis.field_names[name] );
                    }

                    for( var name in response.analysis.field_types )
                    {
                      build_analysis_table( 'type', name, response.analysis.field_types[name] );
                    }

                    check_empty_spacer();
                  },
}, function(error) {
  if (error.status == 404) {
    $scope.analysisHandlerMissing = true;
// @todo    #analysis-handler-missing.show();
  } else {
    $scope.analysisError = error.error.msg;
    // @todo #analysis-error.show();
  }

          var generate_class_name = function( type )
          {
            var classes = [type];
            if( 'text' !== type )
            {
              classes.push( 'verbose_output' );
            }
            return classes.join( ' ' );
          }

          var build_analysis_table = function( field_or_name, name, analysis_data )
          {
            for( var type in analysis_data ) // index or query
            {
              var type_length = analysis_data[type].length;  // number of stages in pipeline
              if( 0 !== type_length )
              {
                var global_elements_count = 0;
                if( 'string' === typeof analysis_data[type][1] )
                {
                  analysis_data[type][1] = [{ 'text': analysis_data[type][1] }]
                }

                for( var i = 1; i < type_length; i += 2 )
                {
                  var tmp_type_length = analysis_data[type][i].length;
                  for( var j = 0; j < tmp_type_length; j++ )
                  {
                    global_elements_count = Math.max
                    (
                      ( analysis_data[type][i][j].positionHistory || [] )[0] || 1,
                      global_elements_count
                    );
                  }
                }

                for( component in components(analysis_data[type]))  // why i+=2??
                {
                  var colspan = 1;
                  var elements = analysis_data[type][i+1];
                  var elements_count = global_elements_count;

                  if( !elements[0] || !elements[0].positionHistory )
                  {
                    colspan = elements_count;
                    elements_count = 1;
                  }

                  var legend = [];
                  for( var key in elements[0] )
                  {
                    var key_parts = key.split( '#' );
                    var used_key = key_parts.pop();
                    var short_key = used_key;

                    if( 1 === key_parts.length )
                    {
                      used_key = '<abbr title="' + key + '">' + used_key + '</abbr>';
                    }

                    if( 'positionHistory' === short_key || 'match' === short_key )
                    {
                      continue;
                    }

                    legend.push
                    (

                    );
                  }


                    // analyzer
                    var analyzer_name = analysis_data[type][i].replace( /(\$1)+$/g, '' );

                    var analyzer_short = -1 !== analyzer_name.indexOf( '$' )
                                       ? analyzer_name.split( '$' )[1]
                                       : analyzer_name.split( '.' ).pop();
                    analyzer_short = analyzer_short.match( /[A-Z]/g ).join( '' );



                    // data
                    var cell_content = '<td class="part data spacer" colspan="' + colspan + '"><div class="holder">&nbsp;</div></td>';
                    var cells = new Array( elements_count + 1 ).join( cell_content );
                    content += cells + "\n";



                $( '.' + type, analysis_result )
                  .remove();

                analysis_result
                  .append( content );

                var analysis_result_type = $( '.' + type, analysis_result );

                for( var i = 0; i < analysis_data[type].length; i += 2 )
                {
                  for( var j = 0; j < analysis_data[type][i+1].length; j += 1 )
                  {
                    var pos = analysis_data[type][i+1][j].positionHistory
                        ? analysis_data[type][i+1][j].positionHistory[0]
                        : 1;
                    var selector = 'tr.step:eq(' + ( i / 2 ) +') '
                                 + 'td.data:eq(' + ( pos - 1 ) + ') '
                                 + '.holder';
                    var cell = $( selector, analysis_result_type );

                    cell.parent()
                      .removeClass( 'spacer' );

                    var table = $( 'table tr.details', cell );
                    if( 0 === table.size() )
                    {
                      cell
                        .html
                        (
                          '<table border="0" cellspacing="0" cellpadding="0">' +
                          '<tr class="details"></tr></table>'
                        );
                      var table = $( 'table tr.details', cell );
                    }

                    var tokens = [];
                    for( var key in analysis_data[type][i+1][j] )
                    {
                      var short_key = key.split( '#' ).pop();

                      if( 'positionHistory' === short_key || 'match' === short_key )
                      {
                        continue;
                      }

                      var classes = [];
                      classes.push( generate_class_name( short_key ) );

                      var data = analysis_data[type][i+1][j][key];
                      if( 'object' === typeof data && data instanceof Array )
                      {
                        data = data.join( ' ' );
                      }
                      if( 'string' === typeof data )
                      {
                        data = data.esc();
                      }

                      if( null === data || 0 === data.length )
                      {
                        classes.push( 'empty' );
                        data = '&empty;';
                      }

                      if( analysis_data[type][i+1][j].match &&
                        ( 'text' === short_key || 'raw_bytes' === short_key ) )
                      {
                        classes.push( 'match' );
                      }

                      tokens.push
                      (
                        '<tr class="' + classes.join( ' ' ) + '">' +
                        '<td>' + data + '</td>' +
                        '</tr>'
                      );
                    }
                    table
                      .append
                      (
                        '<td class="details">' +
                        '<table border="0" cellspacing="0" cellpadding="0">' +
                        tokens.join( "\n" ) +
                        '</table></td>'
                      );
                  }
                }

              }
            }
          }
************/

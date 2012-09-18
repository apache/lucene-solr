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

// #/:core/analysis
sammy.get
(
  /^#\/([\w\d-]+)\/(analysis)$/,
  function( context )
  {
    var active_core = this.active_core;
    var core_basepath = active_core.attr( 'data-basepath' );
    var content_element = $( '#content' );
 
    $.get
    (
      'tpl/analysis.html',
      function( template )
      {
        content_element
          .html( template );
                
        var analysis_element = $( '#analysis', content_element );
        var analysis_form = $( 'form', analysis_element );
        var analysis_result = $( '#analysis-result', analysis_element );
        analysis_result.hide();

        var verbose_link = $( '.verbose_output a', analysis_element );

        var type_or_name = $( '#type_or_name', analysis_form );
        var schema_browser_element = $( '#tor_schema' );
        var schema_browser_path = $( 'p > a', active_core ).attr( 'href' ) + '/schema-browser'
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

        $.ajax
        (
          {
            url : core_basepath + '/admin/luke?wt=json&show=schema',
            dataType : 'json',
            context : type_or_name,
            beforeSend : function( xhr, settings )
            {
              this
                .html( '<option value="">Loading ... </option>' )
                .addClass( 'loader' );
            },
            success : function( response, text_status, xhr )
            {
              var content = '';
                            
              var fields = [];
              for( var field_name in response.schema.fields )
              {
                fields.push
                (
                  '<option value="fieldname=' + field_name + '">' + field_name + '</option>'
                );
              }
              if( 0 !== fields.length )
              {
                content += '<optgroup label="Fields">' + "\n";
                content += fields.sort().join( "\n" ) + "\n";
                content += '</optgroup>' + "\n";
              }
                            
              var types = [];
              for( var type_name in response.schema.types )
              {
                types.push
                (
                  '<option value="fieldtype=' + type_name + '">' + type_name + '</option>'
                );
              }
              if( 0 !== types.length )
              {
                content += '<optgroup label="Types">' + "\n";
                content += types.sort().join( "\n" ) + "\n";
                content += '</optgroup>' + "\n";
              }
                            
              this
                .html( content );

              var defaultSearchField = 'fieldname\=' + ( context.params['analysis.fieldname'] || response.schema.defaultSearchField );

              if( context.params['analysis.fieldtype'] )
              {
                defaultSearchField = 'fieldtype\=' + context.params['analysis.fieldtype'];
              }

              $( 'option[value="' + defaultSearchField + '"]', this )
                .attr( 'selected', 'selected' );

              this
                .chosen()
                .trigger( 'change' );

              var fields = 0;
              for( var key in context.params )
              {
                if( 'string' === typeof context.params[key] )
                {
                  fields++;
                  $( '[name="' + key + '"]', analysis_form )
                    .val( decodeURIComponent( context.params[key].replace( /\+/g, '%20' ) ) );
                }
              }

              if( 'undefined' !== typeof context.params.verbose_output )
              {
                verbose_link.trigger( 'toggle', !!context.params.verbose_output.match( /^(1|true)$/ ) );
              }

              if( 0 !== fields )
              {
                analysis_form
                  .trigger( 'execute' );
              }
            },
            error : function( xhr, text_status, error_thrown)
            {
            },
            complete : function( xhr, text_status )
            {
              this
                .removeClass( 'loader' );
            }
          }
        );
                        
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

        var button = $( 'button', analysis_form )

        var compute_analysis_params = function()
        {
          var params = analysis_form.formToArray();
                          
          var type_or_name = $( '#type_or_name', analysis_form ).val().split( '=' );
          params.push( { name: 'analysis.' + type_or_name[0], value: type_or_name[1] } );
          params.push( { name: 'verbose_output', value: $( '.verbose_output', analysis_element ).hasClass( 'active' ) ? 1 : 0 } );

          return params;
        }
                
        analysis_form
          .die( 'submit' )
          .live
          (
            'submit',
            function( event )
            {
              var params = compute_analysis_params();

              context.redirect( context.path.split( '?' ).shift() + '?' + $.param( params ) );
              return false;
            }
          )
          .die( 'execute' )
          .live
          (
            'execute',
            function( event )
            {
              var url = core_basepath + '/analysis/field?wt=json&analysis.showmatch=true&' + context.path.split( '?' ).pop();
              url = url.replace( /&verbose_output=\d/, '' );

              $.ajax
              (
                {
                  url : url,
                  dataType : 'json',
                  beforeSend : function( xhr, settings )
                  {
                    loader.show( button );
                    button.attr( 'disabled', true );
                  },
                  success : function( response, status_text, xhr, form )
                  {
                    $( '.analysis-error', analysis_element )
                      .hide();
                                    
                    analysis_result
                      .empty()
                      .show();
                                    
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
                  error : function( xhr, text_status, error_thrown )
                  {
                    analysis_result
                      .empty()
                      .hide();

                    if( 404 === xhr.status )
                    {
                      $( '#analysis-handler-missing', analysis_element )
                        .show();
                    }
                    else
                    {
                      $( '#analysis-error', analysis_element )
                        .show();

                      var response = null;
                      try
                      {
                        eval( 'response = ' + xhr.responseText + ';' );
                      }
                      catch( e )
                      {
                        console.error( e );
                      }

                      $( '#analysis-error .body', analysis_element )
                        .text( response ? response.error.msg : xhr.responseText );
                    }
                  },
                  complete : function()
                  {
                    loader.hide( $( 'button', analysis_form ) );
                    button.removeAttr( 'disabled' );
                  }
                }
              );
            }
          );

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
            for( var type in analysis_data )
            {
              var type_length = analysis_data[type].length;
              if( 0 !== type_length )
              {
                var global_elements_count = 0;
                for( var i = 0; i < analysis_data[type].length; i += 2 )
                {
                  if( 'string' === typeof analysis_data[type][i+1] )
                  {
                    analysis_data[type][i+1] = [{ 'text': analysis_data[type][i+1] }]
                  }

                  var tmp = {};
                  var cols = analysis_data[type][i+1].filter
                  (
                    function( obj )
                    {
                      var obj_position = obj.position || 0;
                      if( !tmp[obj_position] )
                      {
                        tmp[obj_position] = true;
                        return true;
                      }

                      return false;
                    }
                  );

                  global_elements_count = Math.max( global_elements_count, cols.length );
                }

                var content = '<div class="' + type + '">' + "\n";
                content += '<table border="0" cellspacing="0" cellpadding="0">' + "\n";
                                
                for( var i = 0; i < analysis_data[type].length; i += 2 )
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
                      '<tr class="' + generate_class_name( short_key ) + '">' +
                      '<td>' + used_key + '</td>' +
                      '</tr>'
                    );
                  }

                  content += '<tbody>' + "\n";
                  content += '<tr class="step">' + "\n";

                    // analyzer
                    var analyzer_name = analysis_data[type][i].replace( /(\$1)+$/g, '' );

                    var analyzer_short = -1 !== analyzer_name.indexOf( '$' )
                                       ? analyzer_name.split( '$' )[1]
                                       : analyzer_name.split( '.' ).pop();
                    analyzer_short = analyzer_short.match( /[A-Z]/g ).join( '' );

                    content += '<td class="part analyzer"><div>' + "\n";
                    content += '<abbr title="' + analysis_data[type][i].esc() + '">' + "\n";
                    content += analyzer_short.esc() + '</abbr></div></td>' + "\n";

                    // legend
                    content += '<td class="part legend"><div class="holder">' + "\n";
                    content += '<table border="0" cellspacing="0" cellpadding="0">' + "\n";
                    content += '<tr><td>' + "\n";
                    content += '<table border="0" cellspacing="0" cellpadding="0">' + "\n";
                    content += legend.join( "\n" ) + "\n";
                    content += '</table></td></tr></table></td>' + "\n";

                    // data
                    var cell_content = '<td class="part data spacer" colspan="' + colspan + '"><div class="holder">&nbsp;</div></td>';
                    var cells = new Array( elements_count + 1 ).join( cell_content );
                    content += cells + "\n";

                  content += '</tr>' + "\n";
                  content += '</tbody>' + "\n";
                }
                content += '</table>' + "\n";
                content += '</div>' + "\n";

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
                    
      }
    );
  }
);

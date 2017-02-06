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

var dataimport_timeout = 2000;
var cookie_dataimport_autorefresh = 'dataimport_autorefresh';

sammy.bind
(
  'dataimport_queryhandler_load',
  function( event, params )
  {
    var core_basepath = params.active_core.attr( 'data-basepath' );

    $.ajax
    (
      {
        url : core_basepath + '/admin/mbeans?cat=QUERY&wt=json',
        dataType : 'json',
        beforeSend : function( xhr, settings )
        {
        },
        success : function( response, text_status, xhr )
        {
          var handlers = response['solr-mbeans'][1];
          var dataimport_handlers = [];
          for( var key in handlers )
          {
            if( handlers[key]['class'] !== key &&
              handlers[key]['class'] === 'org.apache.solr.handler.dataimport.DataImportHandler' )
            {
              dataimport_handlers.push( key );
            }
          }
          params.callback( dataimport_handlers.sort(naturalSort) );
        },
        error : function( xhr, text_status, error_thrown)
        {
        },
        complete : function( xhr, text_status )
        {
        }
      }
    );
  }
);

// #/:core/dataimport
sammy.get
(
  new RegExp( app.core_regex_base + '\\/(dataimport)$' ),
  function( context )
  {
    sammy.trigger
    (
      'dataimport_queryhandler_load',
      {
        active_core : this.active_core,
        callback :  function( dataimport_handlers )
        {
          if( 0 === dataimport_handlers.length )
          {
            $( '#content' )
              .html( 'sorry, no dataimport-handler defined!' );

            return false;
          }

          context.redirect( context.path + '/' + dataimport_handlers[0] );
        }
      }
    );
  }
);

// #/:core/dataimport
sammy.get
(
  new RegExp( app.core_regex_base + '\\/(dataimport)\\/' ),
  function( context )
  {
    var core_basepath = this.active_core.attr( 'data-basepath' );
    var content_element = $( '#content' );

    var path_parts = this.path.match( /^(.+\/dataimport\/)(.*)$/ );
    var handler_url = core_basepath + path_parts[2];
        
    $( 'li.dataimport', this.active_core )
      .addClass( 'active' );

    $.get
    (
      'tpl/dataimport.html',
      function( template )
      {
        content_element
          .html( template );

        var dataimport_element = $( '#dataimport', content_element );
        var form_element = $( '#form', dataimport_element );
        var config_element = $( '#config', dataimport_element );
        var error_element = $( '#error', dataimport_element );
        var debug_response_element = $( '#debug_response', dataimport_element );

        var autorefresh_status = false;
        var debug_mode = false;

        // handler

        sammy.trigger
        (
          'dataimport_queryhandler_load',
          {
            active_core : context.active_core,
            callback :  function( dataimport_handlers )
            {
              var handlers_element = $( '#navigation ul', form_element );
              var handlers = [];

              for( var i = 0; i < dataimport_handlers.length; i++ )
              {
                handlers.push
                (
                    '<li><a href="' + path_parts[1] + dataimport_handlers[i] + '">' +
                    dataimport_handlers[i] +
                    '</a></li>'
                );
              }

              $( handlers_element )
                .html( handlers.join( "\n") ) ;
                            
              $( 'a[href="' + context.path + '"]', handlers_element ).closest( 'li' )
                .addClass( 'current' );

              $( 'form', form_element )
                .show();
            }
          }
        );

        // config

        function dataimport_fetch_config()
        {
          $.ajax
          (
            {
              url : handler_url + '?command=show-config&indent=true',
              dataType : 'xml',
              context : $( '#dataimport_config', config_element ),
              beforeSend : function( xhr, settings )
              {
                error_element
                  .empty()
                  .hide();
              },
              success : function( config, text_status, xhr )
              {
                dataimport_element
                  .removeClass( 'error' );

                config_element
                  .addClass( 'hidden' );

                var entities = [ '<option value=""></option>' ];

                $( 'document > entity', config )
                  .each
                  (
                    function( i, element )
                    {
                      entities.push( '<option>' + $( element ).attr( 'name' ).esc() + '</option>' );
                    }
                  );
                                
                $( '#entity', form_element )
                  .html( entities.join( "\n" ) );

                $( '.editable textarea', this )
                  .val( xhr.responseText.replace( /\n+$/, '' ) );
              },
              error : function( xhr, text_status, error_thrown )
              {
                if( 'parsererror' === error_thrown )
                {
                  dataimport_element
                    .addClass( 'error' );
                                    
                  error_element
                    .text( 'Dataimport XML-Configuration is not valid' )
                    .show();

                  config_element
                    .removeClass( 'hidden' );
                }
              },
              complete : function( xhr, text_status )
              {
                var code = $(
                  '<pre class="syntax language-xml"><code>' +
                  xhr.responseText.esc() +
                  '</code></pre>'
                );
                $( '.formatted', this ).html( code );

                if( 'success' === text_status )
                {
                  hljs.highlightBlock( code.get(0) );
                }
              }
            }
          );
        }
        dataimport_fetch_config();

        $( '.block .toggle', dataimport_element )
          .die( 'click' )
          .live
          (
            'click',
            function( event )
            {
              $( this ).parents( '.block' )
                .toggleClass( 'hidden' );
                            
              return false;
            }
          )

        var reload_config_element = $( '.reload_config', config_element );
        reload_config_element
          .die( 'click' )
          .live
          (
            'click',
            function( event )
            {
              $.ajax
              (
                {
                  url : handler_url + '?command=reload-config',
                  dataType : 'xml',
                  context: $( this ),
                  beforeSend : function( xhr, settings )
                  {
                    this
                      .removeClass( 'error' )
                      .addClass( 'loader' );
                  },
                  success : function( response, text_status, xhr )
                  {
                    this
                      .addClass( 'success' );

                    window.setTimeout
                    (
                      function()
                      {
                        reload_config_element
                          .removeClass( 'success' );
                      },
                      5000
                    );
                  },
                  error : function( xhr, text_status, error_thrown )
                  {
                    this
                      .addClass( 'error' );
                  },
                  complete : function( xhr, text_status )
                  {
                    this
                      .removeClass( 'loader' );
                                        
                    dataimport_fetch_config();
                  }
                }
              );
              return false;
            }
          );

        var debug_mode_element = $( '.debug_mode', config_element );
        debug_mode_element
          .die( 'click' )
          .live
          (
            'click',
            function( event )
            {
              var self = $( this );
              var block = self.closest( '.block' )

              var debug_checkbox = $( 'input[name="debug"]', form_element );
              var submit_span = $( 'button[type="submit"] span', form_element );

              debug_mode = !debug_mode;

              block.toggleClass( 'debug_mode', debug_mode );

              if( debug_mode )
              {
                block.removeClass( 'hidden' );

                debug_checkbox
                  .attr( 'checked', 'checked' )
                  .trigger( 'change' );
                  
                submit_span
                  .data( 'original', submit_span.text() )
                  .text( submit_span.data( 'debugmode' ) );
              }
              else
              {
                submit_span
                  .text( submit_span.data( 'original' ) )
                  .removeData( 'original' );
              }
            }
          );

        // abort

        var abort_import_element = $( '.abort-import', dataimport_element );
        abort_import_element
          .off( 'click' )
          .on
          (
            'click',
            function( event )
            {
              var span_element = $( 'span', this );

              $.ajax
              (
                {
                  url : handler_url + '?command=abort&wt=json',
                  dataType : 'json',
                  type: 'GET',
                  context: $( this ),
                  beforeSend : function( xhr, settings )
                  {
                    span_element
                      .addClass( 'loader' );
                  },
                  success : function( response, text_status, xhr )
                  {
                    span_element
                      .data( 'original', span_element.text() )
                      .text( span_element.data( 'aborting' ) );

                    this
                      .removeClass( 'warn' )
                      .addClass( 'success' );

                    window.setTimeout
                    (
                      function()
                      {
                        $( 'span', abort_import_element )
                          .removeClass( 'loader' )
                          .text( span_element.data( 'original' ) )
                          .removeData( 'original' );

                        abort_import_element
                          .removeClass( 'success' )
                          .addClass( 'warn' );
                      },
                      dataimport_timeout * 2
                    );

                    dataimport_fetch_status();
                  }
                }
              );
              return false;
            }
          );

        // state

        var status_button = $( 'form button.refresh-status', form_element );

        status_button
          .off( 'click' )
          .on
          (
            'click',
            function( event )
            {
              dataimport_fetch_status();
              return false;
            }
          )
          .trigger( 'click' );
                
        function dataimport_fetch_status( clear_timeout )
        {
          if( clear_timeout )
          {
            app.clear_timeout();
          }

          $.ajax
          (
            {
              url : handler_url + '?command=status&indent=true&wt=json',
              dataType : 'json',
              beforeSend : function( xhr, settings )
              {
                $( 'span', status_button )
                  .addClass( 'loader' );
              },
              success : function( response, text_status, xhr )
              {
                var state_element = $( '#current_state', content_element );

                var status = response.status;
                var rollback_time = response.statusMessages.Rolledback || null;
                var abort_time = response.statusMessages.Aborted || null;
                
                var messages = response.statusMessages;
                var messages_count = 0;
                for( var key in messages ) { messages_count++; }

                function dataimport_compute_details( response, details_element, elapsed_seconds )
                {
                  details_element
                    .show();

                  // --

                  var document_config = {
                    'Requests' : 'Total Requests made to DataSource',
                    'Fetched' : 'Total Rows Fetched',
                    'Skipped' : 'Total Documents Skipped',
                    'Processed' : 'Total Documents Processed'
                  };

                  var document_details = [];
                  for( var key in document_config )
                  {
                    var value = parseInt( response.statusMessages[document_config[key]], 10 );

                    var detail = '<abbr title="' + document_config[key].esc() + '">' + key.esc() + '</abbr>: ' +  app.format_number( value ).esc();
                    if( elapsed_seconds && 'skipped' !== key.toLowerCase() )
                    {
                      detail += ' <span>(' + app.format_number( Math.round( value / elapsed_seconds ) ).esc() + '/s)</span>'
                    }

                    document_details.push( detail );
                  };

                  $( '.docs', details_element )
                    .html( document_details.join( ', ' ) );

                  // --

                  var dates_config = {
                      'Started' : 'Full Dump Started',
                      'Aborted' : 'Aborted',
                      'Rolledback' : 'Rolledback'
                  };

                  var dates_details = [];
                  for( var key in dates_config )
                  {
                    var value = response.statusMessages[dates_config[key]];

                    if( value )
                    {
                      var detail = '<abbr title="' + dates_config[key].esc() + '">' + key.esc() + '</abbr>: '
                                 + '<abbr class="time">' +  value.esc() + '</abbr>';
                      dates_details.push( detail );                      
                    }
                  };

                  var dates_element = $( '.dates', details_element );

                  dates_element
                    .html( dates_details.join( ', ' ) );

                  $( '.time', dates_element )
                    .removeData( 'timeago' )
                    .timeago();
                };

                var get_time_taken = function get_default_time_taken()
                {
                  var time_taken_text = response.statusMessages['Time taken'];
                  return app.convert_duration_to_seconds( time_taken_text );
                };

                var get_default_info_text = function default_info_text()
                {
                  var info_text = response.statusMessages[''] || '';

                  // format numbers included in status nicely
                  info_text = info_text.replace
                  (
                    /\d{4,}/g,
                    function( match, position, string )
                    {
                      return app.format_number( parseInt( match, 10 ) );
                    }
                  );

                  var time_taken_text = app.convert_seconds_to_readable_time( get_time_taken() );
                  if( time_taken_text )
                  {
                    info_text += ' (Duration: ' + time_taken_text.esc() + ')';
                  }

                  return info_text;
                };

                var show_info = function show_info( info_text, elapsed_seconds )
                {
                  $( '.info strong', state_element )
                    .text( info_text || get_default_info_text() );

                  $( '.info .details', state_element )
                    .hide();
                };

                var show_full_info = function show_full_info( info_text, elapsed_seconds )
                {
                  show_info( info_text, elapsed_seconds );

                  dataimport_compute_details
                  (
                    response,
                    $( '.info .details', state_element ),
                    elapsed_seconds || get_time_taken()
                  );
                };

                state_element
                  .removeAttr( 'class' );

                var current_time = new Date();
                $( '.last_update abbr', state_element )
                  .text( current_time.toTimeString().split( ' ' ).shift() )
                  .attr( 'title', current_time.toUTCString() );

                $( '.info', state_element )
                  .removeClass( 'loader' );

                if( 'busy' === status )
                {
                  state_element
                    .addClass( 'indexing' );

                  if( autorefresh_status )
                  {
                    $( '.info', state_element )
                      .addClass( 'loader' );
                  }

                  var time_elapsed_text = response.statusMessages['Time Elapsed'];
                  var elapsed_seconds = app.convert_duration_to_seconds( time_elapsed_text );
                  time_elapsed_text = app.convert_seconds_to_readable_time( elapsed_seconds );

                  var info_text = time_elapsed_text
                                ? 'Indexing since ' + time_elapsed_text
                                : 'Indexing ...';

                  show_full_info( info_text, elapsed_seconds );
                }
                else if( rollback_time )
                {
                  state_element
                    .addClass( 'failure' );

                  show_full_info();
                }
                else if( abort_time )
                {
                  state_element
                    .addClass( 'aborted' );

                  show_full_info( 'Aborting current Import ...' );
                }
                else if( 'idle' === status && 0 !== messages_count )
                {
                  state_element
                    .addClass( 'success' );

                  show_full_info();
                }
                else 
                {
                  state_element
                    .addClass( 'idle' );

                  show_info( 'No information available (idle)' );
                }

                // show raw status

                var code = $(
                  '<pre class="syntax language-json"><code>' +
                  app.format_json( xhr.responseText ).esc() +
                  '</code></pre>'
                );

                $( '#raw_output_container', content_element ).html( code );
                hljs.highlightBlock( code.get(0) );

                if( !app.timeout && autorefresh_status )
                {
                  app.timeout = window.setTimeout
                  (
                    function()
                    {
                      dataimport_fetch_status( true )
                    },
                    dataimport_timeout
                  );
                }
              },
              error : function( xhr, text_status, error_thrown )
              {
                console.debug( arguments );

                reload_config_element
                  .addClass( 'error' );
              },
              complete : function( xhr, text_status )
              {
                $( 'span', status_button )
                  .removeClass( 'loader' )
                  .addClass( 'success' );

                window.setTimeout
                (
                  function()
                  {
                    $( 'span', status_button )
                      .removeClass( 'success' );
                  },
                  dataimport_timeout / 2
                );
              }
            }
          );
        }

        // form

        var form = $( 'form', form_element );

        form
          .ajaxForm
          (
            {
              url : handler_url,
              data : {
                wt : 'json',
                indent : 'true'
              },
              dataType : 'json',
              type: 'POST',
              beforeSend : function( xhr, settings )
              {
                $( 'button[type="submit"] span', form_element )
                  .addClass( 'loader' );

                error_element
                  .empty()
                  .hide();
              },
              beforeSubmit : function( array, form, options )
              {
                var entity = $( '#entity', form ).val();
                if( entity.length )
                {
                  array.push( { name : 'entity', value: entity } );
                }

                var start = parseInt( $( '#start', form ).val(), 10 );
                if( start )
                {
                  array.push( { name : 'start', value: start } );
                }

                var rows = parseInt( $( '#rows', form ).val(), 10 );
                if( rows )
                {
                  array.push( { name : 'rows', value: rows } );
                }

                $( 'input:checkbox', form ).not( ':checked' )
                  .each( function( i, input )
                  {
                    array.push( { name: input.name, value: 'false' } );
                  }
                );

                var custom_parameters = $( '#custom_parameters', form ).val();
                if( custom_parameters.length )
                {
                  var params = custom_parameters.split( '&' );
                  for( var i in params )
                  {
                    var tmp = params[i].split( '=' );
                    array.push( { name : tmp[0], value: tmp[1] } );
                  }
                }

                if( debug_mode )
                {
                  array.push( { name: 'dataConfig', value: $( '#dataimport_config .editable textarea' ).val() } );
                }
              },
              success : function( response, text_status, xhr )
              {
              },
              error : function( xhr, text_status, error_thrown )
              {
                var response = null;
                try
                {
                  eval( 'response = ' + xhr.responseText + ';' );
                }
                catch( e ){}

                error_element
                  .text( response.error.msg || 'Unknown Error (Exception w/o Message)' )
                  .show();
              },
              complete : function( xhr, text_status )
              {
                $( 'button[type="submit"] span', form_element )
                  .removeClass( 'loader' );

                var debug = $( 'input[name="debug"]:checked', form );
                if( 0 !== debug.size() )
                {
                  var code = $(
                    '<pre class="syntax language-json"><code>' +
                    app.format_json( xhr.responseText ).esc() +
                    '</code></pre>'
                  );

                  $( '.content', debug_response_element ).html( code );
                  hljs.highlightBlock( code.get(0) );
                }

                dataimport_fetch_status();
              }
            }
          );

        $( 'input[name="debug"]', form )
          .off( 'change' )
          .on
          (
            'change',
            function( event )
            {
              debug_response_element.toggle( this.checked );
            }
          );

        $( '#auto-refresh-status a', form_element )
          .off( 'click' )
          .on
          (
            'click',
            function( event )
            {
              $.cookie( cookie_dataimport_autorefresh, $.cookie( cookie_dataimport_autorefresh ) ? null : true );
              $( this ).trigger( 'state' );

              dataimport_fetch_status();

              return false;
            }
          )
          .off( 'state' )
          .on
          (
            'state',
            function( event )
            {
              autorefresh_status = !!$.cookie( cookie_dataimport_autorefresh );

              $.cookie( cookie_dataimport_autorefresh )
                ? $( this ).addClass( 'on' )
                : $( this ).removeClass( 'on' );
            }
          )
          .trigger( 'state' );
      }
    );
  }
);

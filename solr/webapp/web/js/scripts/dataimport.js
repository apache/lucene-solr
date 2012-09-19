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

var convert_duration_to_seconds = function( str )
{
  var ret = 0;
  var parts = new String( str ).split( '.' ).shift().split( ':' ).reverse();
  var parts_count = parts.length;
    
  for( var i = 0; i < parts_count; i++ )
  {
    ret += parseInt( parts[i], 10 ) * Math.pow( 60, i );
  }

  return ret;
}

var convert_seconds_to_readable_time = function( value )
{
  var text = [];
  value = parseInt( value );

  var minutes = Math.floor( value / 60 );
  var hours = Math.floor( minutes / 60 );

  if( 0 !== hours )
  {
    text.push( hours + 'h' );
    value -= hours * 60 * 60;
    minutes -= hours * 60;
  }

  if( 0 !== minutes )
  {
    text.push( minutes + 'm' );
    value -= minutes * 60;
  }

  if( 0 !== value )
  {
    text.push( value + 's' );
  }

  return text.join( ' ' );
}

sammy.bind
(
  'dataimport_queryhandler_load',
  function( event, params )
  {
    var core_basepath = params.active_core.attr( 'data-basepath' );

    $.ajax
    (
      {
        url : core_basepath + '/admin/mbeans?cat=QUERYHANDLER&wt=json',
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
          params.callback( dataimport_handlers );
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
  /^#\/([\w\d-]+)\/(dataimport)$/,
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
  /^#\/([\w\d-]+)\/(dataimport)\//,
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
        var config_error_element = $( '#config-error', dataimport_element );

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
              url : handler_url + '?command=show-config',
              dataType : 'xml',
              context : $( '#dataimport_config', config_element ),
              beforeSend : function( xhr, settings )
              {
              },
              success : function( config, text_status, xhr )
              {
                dataimport_element
                  .removeClass( 'error' );
                                    
                config_error_element
                  .hide();

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
              },
              error : function( xhr, text_status, error_thrown )
              {
                if( 'parsererror' === error_thrown )
                {
                  dataimport_element
                    .addClass( 'error' );
                                    
                  config_error_element
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
                this.html( code );

                if( 'success' === text_status )
                {
                  hljs.highlightBlock( code.get(0) );
                }
              }
            }
          );
        }
        dataimport_fetch_config();

        $( '.toggle', config_element )
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
          )

        // state
                
        function dataimport_fetch_status()
        {
          $.ajax
          (
            {
              url : handler_url + '?command=status',
              dataType : 'xml',
              beforeSend : function( xhr, settings )
              {
              },
              success : function( response, text_status, xhr )
              {
                var state_element = $( '#current_state', content_element );

                var status = $( 'str[name="status"]', response ).text();
                var rollback_element = $( 'str[name="Rolledback"]', response );
                var messages_count = $( 'lst[name="statusMessages"] str', response ).size();

                var started_at = $( 'str[name="Full Dump Started"]', response ).text();
                if( !started_at )
                {
                  started_at = (new Date()).toGMTString();
                }

                function dataimport_compute_details( response, details_element )
                {
                  var details = [];
                                    
                  var requests = parseInt( $( 'str[name="Total Requests made to DataSource"]', response ).text(), 10 );
                  if( requests )
                  {
                    details.push
                    (
                      '<abbr title="Total Requests made to DataSource">Requests</abbr>: ' +
                      requests
                    );
                  }

                  var fetched = parseInt( $( 'str[name="Total Rows Fetched"]', response ).text(), 10 );
                  if( fetched )
                  {
                    details.push
                    (
                      '<abbr title="Total Rows Fetched">Fetched</abbr>: ' +
                      fetched
                    );
                  }

                  var skipped = parseInt( $( 'str[name="Total Documents Skipped"]', response ).text(), 10 );
                  if( requests )
                  {
                    details.push
                    (
                      '<abbr title="Total Documents Skipped">Skipped</abbr>: ' +
                      skipped
                    );
                  }

                  var processed = parseInt( $( 'str[name="Total Documents Processed"]', response ).text(), 10 );
                  if( processed )
                  {
                    details.push
                    (
                      '<abbr title="Total Documents Processed">Processed</abbr>: ' +
                      processed
                    );
                  }

                  details_element
                    .html( details.join( ', ' ) )
                    .show();
                }

                state_element
                  .removeClass( 'indexing' )
                  .removeClass( 'success' )
                  .removeClass( 'failure' );
                                
                $( '.info', state_element )
                  .removeClass( 'loader' );

                if( 0 !== rollback_element.size() )
                {
                  state_element
                    .addClass( 'failure' )
                    .show();

                  $( '.time', state_element )
                    .text( rollback_element.text() )
                    .timeago()
                    .show();

                  $( '.info strong', state_element )
                    .text( $( 'str[name=""]', response ).text() );

                  $( '.info .details', state_element )
                    .hide();
                                    
                  console.debug( 'rollback @ ', rollback_element.text() );
                }
                else if( 'idle' === status && 0 !== messages_count )
                {
                  state_element
                    .addClass( 'success' )
                    .show();

                  $( '.time', state_element )
                    .text( started_at )
                    .timeago()
                    .show();

                  $( '.info strong', state_element )
                    .text( $( 'str[name=""]', response ).text() );

                  dataimport_compute_details( response, $( '.info .details', state_element ) );
                }
                else if( 'busy' === status )
                {
                  state_element
                    .addClass( 'indexing' )
                    .show();

                  $( '.time', state_element )
                    .text( started_at )
                    .timeago()
                    .show();

                  $( '.info', state_element )
                    .addClass( 'loader' );

                  var indexing_text = 'Indexing ...';

                  var time_elapsed_text = $( 'str[name="Time Elapsed"]', response ).text();
                  time_elapsed_text = convert_seconds_to_readable_time( convert_duration_to_seconds( time_elapsed_text ) );
                  if( time_elapsed_text.length )
                  {
                    indexing_text = 'Indexing since ' + time_elapsed_text
                  }

                  $( '.info strong', state_element )
                    .text( indexing_text );
                                    
                  dataimport_compute_details( response, $( '.info .details', state_element ) );

                  window.setTimeout( dataimport_fetch_status, 2000 );
                }
                else
                {
                  state_element.hide();
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
              }
            }
          );
        }
        dataimport_fetch_status();

        // form

        $( 'form', form_element )
          .ajaxForm
          (
            {
              url : handler_url,
              dataType : 'xml',
              beforeSend : function( xhr, settings )
              {
                $( 'form button', form_element )
                  .addClass( 'loader' );
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
              },
              success : function( response, text_status, xhr )
              {
                dataimport_fetch_status();
              },
              error : function( xhr, text_status, error_thrown )
              {
                console.debug( arguments );
              },
              complete : function( xhr, text_status )
              {
                $( 'form button', form_element )
                  .removeClass( 'loader' );
              }
            }
          );
      }
    );
  }
);
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

var loader = {
    
  show : function( element )
  {
    $( element )
      .addClass( 'loader' );
  },
    
  hide : function( element )
  {
    $( element )
      .removeClass( 'loader' );
  }
    
};

Number.prototype.esc = function()
{
  return new String( this ).esc();
}

String.prototype.esc = function()
{
  return this.replace( /</g, '&lt;' ).replace( />/g, '&gt;' );
}

SolrDate = function( date )
{
  // ["Sat Mar 03 11:00:00 CET 2012", "Sat", "Mar", "03", "11:00:00", "CET", "2012"]
  var parts = date.match( /^(\w+)\s+(\w+)\s+(\d+)\s+(\d+\:\d+\:\d+)\s+(\w+)\s+(\d+)$/ );
    
  // "Sat Mar 03 2012 10:37:33"
  return new Date( parts[1] + ' ' + parts[2] + ' ' + parts[3] + ' ' + parts[6] + ' ' + parts[4] );
}

var sammy = $.sammy
(
  function()
  {
    this.bind
    (
      'run',
      function( event, config )
      {
        if( 0 === config.start_url.length )
        {
          location.href = '#/';
          return false;
        }
      }
    );

    this.bind
    (
      'error',
      function( message, original_error )
      {
        alert( original_error.message );
      }
    );
        
    // activate_core
    this.before
    (
      {},
      function( context )
      {
        app.clear_timeout();

        var menu_wrapper = $( '#menu-wrapper' );

        $( 'li[id].active', menu_wrapper )
          .removeClass( 'active' );
                
        $( 'li.active', menu_wrapper )
          .removeClass( 'active' );

        // global dashboard doesn't have params.splat
        if( !this.params.splat )
        {
          this.params.splat = [ '~index' ];
        }

        var selector = '~' === this.params.splat[0][0]
                     ? '#' + this.params.splat[0].replace( /^~/, '' ) + '.global'
                     : '#core-selector #' + this.params.splat[0].replace( /\./g, '__' );

        var active_element = $( selector, menu_wrapper );
                  
        if( 0 === active_element.size() )
        {
          this.app.error( 'There exists no core with name "' + this.params.splat[0] + '"' );
          return false;
        }

        if( active_element.hasClass( 'global' ) )
        {
          active_element
            .addClass( 'active' );

          if( this.params.splat[1] )
          {
            $( '.' + this.params.splat[1], active_element )
              .addClass( 'active' );
          }

          $( '#core-selector option[selected]' )
            .removeAttr( 'selected' )
            .trigger( 'liszt:updated' );

          $( '#core-selector .chzn-container > a' )
            .addClass( 'chzn-default' );
        }
        else
        {
          active_element
            .attr( 'selected', 'selected' )
            .trigger( 'liszt:updated' );

          if( !this.params.splat[1] )
          {
            this.params.splat[1] = 'overview';
          }

          $( '#core-menu .' + this.params.splat[1] )
            .addClass( 'active' );

          this.active_core = active_element;
        }

        check_fixed_menu();
      }
    );
  }
);

var solr_admin = function( app_config )
{
  that = this,

  menu_element = null,

  is_multicore = null,
  cores_data = null,
  active_core = null,
    
  config = app_config,
  params = null,
  dashboard_values = null,
  schema_browser_data = null,

  plugin_data = null,
    
  this.menu_element = $( '#core-selector select' );
  this.core_menu = $( '#core-menu ul' );

  this.config = config;
  this.timeout = null;

  this.core_regex_base = '^#\\/([\\w\\d-\\.]+)';

  browser = {
    locale : null,
    language : null,
    country : null
  };

  show_global_error = function( error )
  {
    var main = $( '#main' );

    $( 'div[id$="-wrapper"]', main )
      .remove();

    main
      .addClass( 'error' )
      .append( error );

    var pre_tags = $( 'pre', main );
    if( 0 !== pre_tags.size() )
    {
      hljs.highlightBlock( pre_tags.get(0) ); 
    }
  };

  sort_cores_data = function sort_cores_data( cores_status )
  {
    // build array of core-names for sorting
    var core_names = [];
    for( var core_name in cores_status )
    {
      core_names.push( core_name );
    }
    core_names.sort();

    var core_count = core_names.length;
    var cores = {};

    for( var i = 0; i < core_count; i++ )
    {
      var core_name = core_names[i];
      cores[core_name] = cores_status[core_name];
    }

    return cores;
  };

  this.set_cores_data = function set_cores_data( cores )
  {
    that.cores_data = sort_cores_data( cores.status );
    
    that.menu_element
      .empty();

    var core_list = [];
    core_list.push( '<option></option>' );

    var core_count = 0;
    for( var core_name in that.cores_data )
    {
      core_count++;
      var core_path = config.solr_path + '/' + core_name;
      var classes = [];

      if( cores.status[core_name]['isDefaultCore'] )
      {
        classes.push( 'default' );
      }

      var core_tpl = '<option '
                   + '    id="' + core_name.replace( /\./g, '__' ) + '" '
                   + '    class="' + classes.join( ' ' ) + '"'
                   + '    data-basepath="' + core_path + '"'
                   + '    schema="' + cores.status[core_name]['schema'] + '"'
                   + '    config="' + cores.status[core_name]['config'] + '"'
                   + '    value="#/' + core_name + '"'
                   + '    title="' + core_name + '"'
                   + '>' 
                   + core_name 
                   + '</option>';

      core_list.push( core_tpl );
    }

    var has_cores = 0 !== core_count;
    if( has_cores )
    {
      that.menu_element
        .append( core_list.join( "\n" ) )
        .trigger( 'liszt:updated' );
    }

    var core_selector = $( '#core-selector' );
    core_selector.find( '#has-cores' ).toggle( has_cores );
    core_selector.find( '#has-no-cores' ).toggle( !has_cores );

    if( has_cores )
    {
      var cores_element = core_selector.find( '#has-cores' );
      var selector_width = cores_element.width();

      cores_element.find( '.chzn-container' )
        .css( 'width', selector_width + 'px' );
      
      cores_element.find( '.chzn-drop' )
        .css( 'width', ( selector_width - 2 ) + 'px' );
    }

    this.check_for_init_failures( cores );
  };

  this.remove_init_failures = function remove_init_failures()
  {
    $( '#init-failures' )
      .hide()
      .find( 'ul' )
        .empty();
  }

  this.check_for_init_failures = function check_for_init_failures( cores )
  {
    if( !cores.initFailures )
    {
      this.remove_init_failures();
      return false;
    }

    var failures = [];
    for( var core_name in cores.initFailures )
    {
      failures.push
      (
        '<li>' +
          '<strong>' + core_name.esc() + ':</strong>' + "\n" +
          cores.initFailures[core_name].esc() + "\n" +
        '</li>'
      );
    }

    if( 0 === failures.length )
    {
      this.remove_init_failures();
      return false;
    }

    $( '#init-failures' )
      .show()
      .find( 'ul' )
        .html( failures.join( "\n" ) );
  }

  this.run = function()
  {
    var navigator_language = navigator.userLanguage || navigator.language;
    var language_match = navigator_language.match( /^(\w{2})([-_](\w{2}))?$/ );
    if( language_match )
    {
      if( language_match[1] )
      {
        browser.language = language_match[1].toLowerCase();
      }
      if( language_match[3] )
      {
        browser.country = language_match[3].toUpperCase();
      }
      if( language_match[1] && language_match[3] )
      {
        browser.locale = browser.language + '_' + browser.country
      }
    }

    $.ajax
    (
      {
        url : config.solr_path + config.core_admin_path + '?wt=json&indexInfo=false',
        dataType : 'json',
        beforeSend : function( arr, form, options )
        {               
          $( '#content' )
            .html( '<div id="index"><div class="loader">Loading ...</div></div>' );
        },
        success : function( response )
        {
          that.set_cores_data( response );

          that.menu_element
            .chosen()
            .off( 'change' )
            .on
            (
              'change',
              function( event )
              {
                location.href = $( 'option:selected', this ).val();
                return false;
              }
            )
            .on
            (
              'liszt:updated',
              function( event )
              {
                var core_name = $( 'option:selected', this ).text();

                that.core_menu
                  .html
                  (
                    //Keep this in alphabetical order after the overview
                    '<li class="overview"><a href="#/' + core_name + '"><span>Overview</span></a></li>' + "\n" +
                    '<li class="analysis"><a href="#/' + core_name + '/analysis"><span>Analysis</span></a></li>' + "\n" +
                    '<li class="dataimport"><a href="#/' + core_name + '/dataimport"><span>Dataimport</span></a></li>' + "\n" +
                    '<li class="documents"><a href="#/' + core_name + '/documents"><span>Documents</span></a></li>' + "\n" +
                    '<li class="files"><a href="#/' + core_name + '/files"><span>Files</span></a></li>' + "\n" +
                    '<li class="ping"><a rel="' + that.config.solr_path + '/' + core_name + '/admin/ping"><span>Ping</span></a></li>' + "\n" +
                    '<li class="plugins"><a href="#/' + core_name + '/plugins"><span>Plugins / Stats</span></a></li>' + "\n" +
                    '<li class="query"><a href="#/' + core_name + '/query"><span>Query</span></a></li>' + "\n" +
                    '<li class="replication"><a href="#/' + core_name + '/replication"><span>Replication</span></a></li>' + "\n" +
                    '<li class="schema-browser"><a href="#/' + core_name + '/schema-browser"><span>Schema Browser</span></a></li>' +
                    '<li class="segments"><a href="#/' + core_name + '/segments"><span>Segments info</span></a></li>'
                  )
                  .show();

                if( !core_name )
                {
                  that.core_menu
                    .hide()
                    .empty();
                }
              }
            );

          check_fixed_menu();
          $( window ).resize( check_fixed_menu );

          var system_url = config.solr_path + '/admin/info/system?wt=json';
          $.ajax
          (
            {
              url : system_url,
              dataType : 'json',
              beforeSend : function( arr, form, options )
              {
              },
              success : function( response )
              {
                that.dashboard_values = response;

                var environment_args = null;
                var cloud_args = null;

                if( response.jvm && response.jvm.jmx && response.jvm.jmx.commandLineArgs )
                {
                  var command_line_args = response.jvm.jmx.commandLineArgs.join( ' | ' );

                  environment_args = command_line_args.match( /-Dsolr.environment=((dev|test|prod)?[\w\d]*)/i );
                }

                if( response.mode )
                {
                  cloud_args = response.mode.match( /solrcloud/i );
                }

                // environment

                var wrapper = $( '#wrapper' );
                var environment_element = $( '#environment' );
                if( environment_args )
                {
                  wrapper
                    .addClass( 'has-environment' );

                  if( environment_args[1] )
                  {
                    environment_element
                      .html( environment_args[1] );
                  }

                  if( environment_args[2] )
                  {
                    environment_element
                      .addClass( environment_args[2] );
                  }
                }
                else
                {
                  wrapper
                    .removeClass( 'has-environment' );
                }

                // cloud

                var cloud_nav_element = $( '#menu #cloud' );
                if( cloud_args )
                {
                  cloud_nav_element
                    .show();
                }

                // sammy

                sammy.run( location.hash );
              },
              error : function()
              {
              },
              complete : function()
              {
                loader.hide( this );
              }
            }
          );
        },
        error : function()
        {
        },
        complete : function()
        {
        }
      }
    );
  };

  this.convert_duration_to_seconds = function convert_duration_to_seconds( str )
  {
    var seconds = 0;
    var arr = new String( str || '' ).split( '.' );
    var parts = arr[0].split( ':' ).reverse();
    var parts_count = parts.length;

    for( var i = 0; i < parts_count; i++ )
    {
      seconds += ( parseInt( parts[i], 10 ) || 0 ) * Math.pow( 60, i );
    }

    // treat more or equal than .5 as additional second
    if( arr[1] && 5 <= parseInt( arr[1][0], 10 ) )
    {
      seconds++;
    }

    return seconds;
  };

  this.convert_seconds_to_readable_time = function convert_seconds_to_readable_time( seconds )
  {
    seconds = parseInt( seconds || 0, 10 );
    var minutes = Math.floor( seconds / 60 );
    var hours = Math.floor( minutes / 60 );

    var text = [];
    if( 0 !== hours )
    {
      text.push( hours + 'h' );
      seconds -= hours * 60 * 60;
      minutes -= hours * 60;
    }

    if( 0 !== minutes )
    {
      text.push( minutes + 'm' );
      seconds -= minutes * 60;
    }

    if( 0 !== seconds )
    {
      text.push( ( '0' + seconds ).substr( -2 ) + 's' );
    }

    return text.join( ' ' );
  };

  this.clear_timeout = function clear_timeout()
  {
    if( !app.timeout )
    {
      return false;
    }

    console.debug( 'Clearing Timeout #' + this.timeout );
    clearTimeout( this.timeout );
    this.timeout = null;
  };

  this.format_json = function format_json( json_str )
  {
    if( JSON.stringify && JSON.parse )
    {
      json_str = JSON.stringify( JSON.parse( json_str ), undefined, 2 );
    }

    return json_str.esc();
  };

  this.format_number = function format_number( number )
  {
    var sep = {
      'de_CH' : '\'',
      'de' : '.',
      'en' : ',',
      'es' : '.',
      'it' : '.',
      'ja' : ',',
      'sv' : ' ',
      'tr' : '.',
      '_' : '' // fallback
    };

    return ( number || 0 ).toString().replace
    (
      /\B(?=(\d{3})+(?!\d))/g,
      sep[ browser.locale ] || sep[ browser.language ] || sep['_']
    );
  };

  check_fixed_menu = function check_fixed_menu()
  {
    $( '#wrapper' ).toggleClass( 'scroll', $( window ).height() < $( '#menu-wrapper' ).height() + $( '#header' ).height() + 40 );
  }

};

var connection_check_delay = 1000;
var connection_working = true;

var connection_check = function connection_check()
{
  $.ajax
  (
    {
      url : config.solr_path + config.core_admin_path + '?wt=json&indexInfo=false',
      dataType : 'json',
      context : $( '.blockUI #connection_status span' ),
      beforeSend : function( arr, form, options )
      {               
        this
          .addClass( 'loader' );
      },
      success : function( response )
      {
        connection_working = true;

        this
          .html( 'Instance is available - <a href="javascript:location.reload();">Reload the page</a>' );

        this.parents( '#connection_status' )
          .addClass( 'online' );

        this.parents( '.blockUI' )
          .css( 'borderColor', '#080' );
      },
      error : function()
      {
        connection_check_delay += connection_check_delay;
        window.setTimeout( connection_check, connection_check_delay );
      },
      complete : function()
      {
        this
          .removeClass( 'loader' );
      }
    }
  );
};

var connection_error = function connection_error()
{
  connection_working = false;

  $.blockUI
  (
    {
      message: $( '#connection_status' ),
      css: { width: '450px', borderColor: '#f00' }
    }
  );

  window.setTimeout( connection_check, connection_check_delay );
}

$( document ).ajaxError
(
  function( event, xhr, settings, thrownError )
  {
    if( connection_working && 0 === xhr.status )
    {
      connection_error();
    }
  }
);

$.ajaxSetup( { cache: false } );
var app = new solr_admin( app_config );

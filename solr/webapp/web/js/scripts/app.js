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
        var menu_wrapper = $( '#menu-wrapper' );

        $( 'li[id].active', menu_wrapper )
          .removeClass( 'active' );
                
        $( 'li.active', menu_wrapper )
          .removeClass( 'active' );

        if( this.params.splat )
        {
          var active_element = $( '#' + this.params.splat[0], menu_wrapper );
                    
          if( 0 === active_element.size() )
          {
            this.app.error( 'There exists no core with name "' + this.params.splat[0] + '"' );
            return false;
          }

          active_element
            .addClass( 'active' );

          if( this.params.splat[1] )
          {
            $( '.' + this.params.splat[1], active_element )
              .addClass( 'active' );
          }

          if( !active_element.hasClass( 'global' ) )
          {
            this.active_core = active_element;
          }
        }
      }
    );
  }
);

var solr_admin = function( app_config )
{
	self = this,

  menu_element = null,

  is_multicore = null,
  cores_data = null,
  active_core = null,
  environment_basepath = null,
    
  config = app_config,
  params = null,
  dashboard_values = null,
  schema_browser_data = null,

  plugin_data = null,
    
  this.menu_element = $( '#menu-selector' );
  this.config = config;

  this.run = function()
  {
    $.ajax
    (
      {
        url : config.solr_path + config.core_admin_path + '?wt=json',
        dataType : 'json',
        beforeSend : function( arr, form, options )
        {               
          $( '#content' )
            .html( '<div id="index"><div class="loader">Loading ...</div></div>' );
        },
        success : function( response )
        {
          self.cores_data = response.status;

          var core_count = 0; for( var i in response.status ) { core_count++; }
          is_multicore = core_count > 1;

          if( is_multicore )
          {
            self.menu_element
              .addClass( 'multicore' );

            $( '#cores', menu_element )
              .show();
          }
          else
          {
            self.menu_element 
              .addClass( 'singlecore' );
          }

          for( var core_name in response.status )
          {
            var core_path = config.solr_path + '/' + core_name;
            var schema =  response['status'][core_name]['schema'];
            var solrconfig =  response['status'][core_name]['config'];
			
            if( !core_name )
            {
              core_name = 'singlecore';
              core_path = config.solr_path
            }

            if( !environment_basepath )
            {
              environment_basepath = core_path;
            }

            var core_tpl = '<li id="' + core_name + '" data-basepath="' + core_path + '" schema="' + schema + '" config="' + solrconfig + '">' + "\n"
                         + '  <p><a href="#/' + core_name + '">' + core_name + '</a></p>' + "\n"
                         + '  <ul>' + "\n"

                         + '    <li class="ping"><a rel="' + core_path + '/admin/ping"><span>Ping</span></a></li>' + "\n"
                         + '    <li class="query"><a href="#/' + core_name + '/query"><span>Query</span></a></li>' + "\n"
                         + '    <li class="schema"><a href="#/' + core_name + '/schema"><span>Schema</span></a></li>' + "\n"
                         + '    <li class="config"><a href="#/' + core_name + '/config"><span>Config</span></a></li>' + "\n"
                         + '    <li class="replication"><a href="#/' + core_name + '/replication"><span>Replication</span></a></li>' + "\n"
                         + '    <li class="analysis"><a href="#/' + core_name + '/analysis"><span>Analysis</span></a></li>' + "\n"
                         + '    <li class="schema-browser"><a href="#/' + core_name + '/schema-browser"><span>Schema Browser</span></a></li>' + "\n"
                         + '    <li class="plugins"><a href="#/' + core_name + '/plugins"><span>Plugins</span></a></li>' + "\n"
                         + '    <li class="dataimport"><a href="#/' + core_name + '/dataimport"><span>Dataimport</span></a></li>' + "\n"

                         + '    </ul>' + "\n"
                         + '</li>';

            self.menu_element
              .append( core_tpl );
          }

          $.ajax
          (
            {
              url : environment_basepath + '/admin/system?wt=json',
              dataType : 'json',
              beforeSend : function( arr, form, options )
              {
              },
              success : function( response )
              {
                self.dashboard_values = response;

                var environment_args = null;
                var cloud_args = null;

                if( response.jvm && response.jvm.jmx && response.jvm.jmx.commandLineArgs )
                {
                  var command_line_args = response.jvm.jmx.commandLineArgs.join( ' | ' );

                  environment_args = command_line_args.match( /-Dsolr.environment=((dev|test|prod)?[\w\d]*)/i );
                  cloud_args = command_line_args.match( /-Dzk/i );
                }

                // title

                $( 'title', document )
                  .append( ' (' + response.core.host + ')' );

                // environment

                var environment_element = $( '#environment' );
                if( environment_args )
                {
                  environment_element
                    .show();

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
                  environment_element
                    .remove();
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
                var main = $( '#main' );

                $( 'div[id$="-wrapper"]', main )
                  .remove();

                main
                  .addClass( 'error' )
                  .append
                  (
                    '<div class="message">This interface requires that you activate the admin request handlers, add the following configuration to your <code>solrconfig.xml:</code></div>' +
                    '<div class="code"><pre class="syntax language-xml"><code>' +
                    '<!-- Admin Handlers - This will register all the standard admin RequestHandlers. -->'.esc() + "\n" +
                    '<requestHandler name="/admin/" class="solr.admin.AdminHandlers" />'.esc() +
                    '</code></pre></div>'
                  );

                hljs.highlightBlock( $( 'pre', main ).get(0) );
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
  }

};

var app = new solr_admin( app_config );
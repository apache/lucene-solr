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

sammy.bind
(
  'plugins_load',
  function( event, params )
  {
    var callback = function()
    {
      params.callback( app.plugin_data.plugin_data, app.plugin_data.sort_table, app.plugin_data.types );
    }
        
    if( app.plugin_data )
    {
      callback( app.plugin_data );
      return true;
    }

    var core_basepath = params.active_core.attr( 'data-basepath' );
    $.ajax
    (
      {
        url : core_basepath + '/admin/mbeans?stats=true&wt=json',
        dataType : 'json',
        beforeSend : function( xhr, settings )
        {
        },
        success : function( response, text_status, xhr )
        {
          var types = [];
          var sort_table = {};
          var plugin_data = {};

          var types_obj = {};
          var plugin_key = null;

          for( var i = 0; i < response['solr-mbeans'].length; i++ )
          {
            if( !( i % 2 ) )
            {
              plugin_key = response['solr-mbeans'][i];
            }
            else
            {
              plugin_data[plugin_key] = response['solr-mbeans'][i];
            }
          }

          for( var key in plugin_data )
          {
            sort_table[key] = {
              url : [],
              component : [],
              handler : []
            };
            for( var part_key in plugin_data[key] )
            {
              if( 0 < part_key.indexOf( '.' ) )
              {
                types_obj[key] = true;
                sort_table[key]['handler'].push( part_key );
              }
              else if( 0 === part_key.indexOf( '/' ) )
              {
                types_obj[key] = true;
                sort_table[key]['url'].push( part_key );
              }
              else
              {
                types_obj[key] = true;
                sort_table[key]['component'].push( part_key );
              }
            }
          }

          for( var type in types_obj )
          {
            types.push( type );
          }
          types.sort();
                    
          app.plugin_data = {
            'plugin_data' : plugin_data,
            'sort_table' : sort_table,
            'types' : types
          }

          $.get
          (
            'tpl/plugins.html',
            function( template )
            {
              $( '#content' )
                .html( template );
                            
              callback( app.plugin_data );
            }
          );
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

// #/:core/plugins/$type
sammy.get
(
  /^#\/([\w\d-]+)\/(plugins)\/(\w+)$/,
  function( context )
  {
    var content_element = $( '#content' );
    var type = context.params.splat[2].toUpperCase();
    var context_path = context.path.split( '?' ).shift();

    sammy.trigger
    (
      'plugins_load',
      {
        active_core : this.active_core,
        callback : function( plugin_data, plugin_sort, types )
        {
          var frame_element = $( '#frame', content_element );
          var navigation_element = $( '#navigation ul', content_element );

          var navigation_content = [];
          for( var i = 0; i < types.length; i++ )
          {
            var type_url = context.params.splat[0] + '/' + context.params.splat[1] + '/' + types[i].toLowerCase();

            navigation_content.push
            (
              '<li class="' + types[i].toLowerCase() + '">' +
              '<a href="#/' + type_url + '">' + types[i] + '</a>' +
              '</li>'
            );
          }

          navigation_element
            .html( navigation_content.join( "\n" ) );
                    
          $( 'a[href="' + context_path + '"]', navigation_element )
            .parent().addClass( 'current' );
                    
          var content = '<ul>';
          for( var sort_key in plugin_sort[type] )
          {
            plugin_sort[type][sort_key].sort();
            var plugin_type_length = plugin_sort[type][sort_key].length;
                        
            for( var i = 0; i < plugin_type_length; i++ )
            {
              content += '<li class="entry">' + "\n";
              content += '<a href="' + context_path + '?entry=' + plugin_sort[type][sort_key][i] + '">';
              content += plugin_sort[type][sort_key][i]
              content += '</a>' + "\n";
              content += '<ul class="detail">' + "\n";
                            
              var details = plugin_data[type][ plugin_sort[type][sort_key][i] ];
              for( var detail_key in details )
              {
                if( 'stats' !== detail_key )
                {
                  var detail_value = details[detail_key];

                  if( 'description' === detail_key )
                  {
                    detail_value = detail_value.replace( /,/g, ',&#8203;' );
                  }
                  else if( 'src' === detail_key )
                  {
                    detail_value = detail_value.replace( /\//g, '/&#8203;' );
                  }

                  content += '<li><dl class="clearfix">' + "\n";
                  content += '<dt>' + detail_key + ':</dt>' + "\n";
                  content += '<dd>' + detail_value + '</dd>' + "\n";
                  content += '</dl></li>' + "\n";
                }
                else if( 'stats' === detail_key && details[detail_key] )
                {
                  content += '<li class="stats clearfix">' + "\n";
                  content += '<span>' + detail_key + ':</span>' + "\n";
                  content += '<ul>' + "\n";

                  for( var stats_key in details[detail_key] )
                  {
                    var stats_value = details[detail_key][stats_key];

                    if( 'readerDir' === stats_key )
                    {
                      stats_value = stats_value.replace( /@/g, '@&#8203;' );
                    }

                    content += '<li><dl class="clearfix">' + "\n";
                    content += '<dt>' + stats_key + ':</dt>' + "\n";
                    content += '<dd>' + stats_value + '</dd>' + "\n";
                    content += '</dl></li>' + "\n";
                  }

                  content += '</ul></li>' + "\n";
                }
              }
                            
              content += '</ul>' + "\n";
            }
          }
          content += '</ul>' + "\n";

          frame_element
            .html( content );

          $( 'a[href="' + decodeURIComponent( context.path ) + '"]', frame_element )
            .parent().addClass( 'expanded' );
                    
          $( '.entry', frame_element )
            .each
            (
              function( i, entry )
              {
                $( '.detail > li', entry ).not( '.stats' ).filter( ':even' )
                  .addClass( 'odd' );

                $( '.stats li:odd', entry )
                  .addClass( 'odd' );
              }
            );
        }
      }
    );                
  }
);

// #/:core/plugins
sammy.get
(
  /^#\/([\w\d-]+)\/(plugins)$/,
  function( context )
  {
    delete app.plugin_data;

    sammy.trigger
    (
      'plugins_load',
      {
        active_core : this.active_core,
        callback :  function( plugin_data, plugin_sort, types )
        {
          context.redirect( context.path + '/' + types[0].toLowerCase() );
        }
      }
    );
  }
);
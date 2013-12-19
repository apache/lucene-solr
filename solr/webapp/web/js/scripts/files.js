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

// #/:core/files
sammy.get
(
  new RegExp( app.core_regex_base + '\\/(files)$' ),
  function( context )
  {
    core_basepath = this.active_core.attr( 'data-basepath' );
    current_core = context.params.splat[0];

    var content_element = $( '#content' );

    var file_endpoint = core_basepath + '/admin/file';

    var path = context.path.split( '?' );
    var selected_file = null;
    if( path && path[1] )
    {
      selected_file = path[1].split( '=' ).pop();
    }

    $.get
    (
      'tpl/files.html',
      function( template )
      {
        content_element
          .html( template );

        var frame_element = $( '#frame', content_element );

        var tree_callback = function( event, data )
        {
          $( 'li[data-file].jstree-closed', event.currentTarget )
            .filter
            (
              function( index, element )
              {
                return selected_file && 0 === selected_file.indexOf( $( element ).data( 'file' ) );
              }
            )
            .each
            (
              function( index, element )
              {
                data.inst.open_node( element );
              }
            );

          if( selected_file )
          {
            $( 'li[data-file="' + selected_file.replace( /\/$/, '' ) + '"] > a', event.currentTarget )
              .addClass( 'active' );
          }
        };

        var load_tree = function()
        {
          $( '#tree', frame_element )
            .empty()
            .jstree
            (
              {
                plugins : [ 'json_data', 'sort' ],
                json_data : {
                  ajax: {
                    url : file_endpoint + '?wt=json',
                    data : function( n )
                    {
                      if( -1 === n )
                        return null;

                      return {
                        file : n.attr( 'data-file' )
                      };
                    },
                    success : function( response, status, xhr )
                    {
                      var files = [];

                      for( var file in response.files )
                      {
                        var is_directory = response.files[file].directory;
                        var prefix = xhr.data ? xhr.data.file + '/' : ''

                        var item = {
                          data: {
                            title : file,
                            attr : {
                              href : '#/' + current_core + '/files?file=' + prefix + file
                            }
                          },
                          attr : {
                            'data-file' : prefix + file
                          }
                        };

                        if( is_directory )
                        {
                          item.state = 'closed';
                          item.data.attr.href += '/';
                        }

                        files.push( item );
                      }

                      return files;
                    }
                  },
                  progressive_render : true
                },
                core : {
                  animation : 0
                }
              }
            )
            .on
            (
              'loaded.jstree',
              tree_callback
            )
            .on
            (
              'open_node.jstree',
              tree_callback
            );
        };
        load_tree();

        if( selected_file )
        {
          $( '#new-file-holder input' )
            .val
            (
              '/' !== selected_file.substr( -1 )
              ? selected_file.replace( /[^\/]+$/, '' )
              : selected_file
            );
        }

        if( selected_file && '/' !== selected_file.substr( -1 ) )
        {
          frame_element
            .addClass( 'show' );

          var endpoint = file_endpoint + '?file=' + selected_file;

          var content_type_map = { xml : 'text/xml', html : 'text/html', js : 'text/javascript' };
          var file_ext = selected_file.match( /\.(\w+)$/  );
          endpoint += '&contentType=' + ( content_type_map[ file_ext[1] || '' ] || 'text/plain' ) + ';charset=utf-8';

          var public_url = window.location.protocol + '//' + window.location.host + endpoint;

          $( '#url', frame_element )
            .text( public_url )
            .attr( 'href', public_url );

          var load_file = function( load_tree )
          {
            if( load_tree )
            {
              load_tree();
            }

            $.ajax
            (
              {
                url : endpoint,
                context : frame_element,
                beforeSend : function( xhr, settings )
                {
                  var block = $( '.view-file .response', this );

                  if( !block.data( 'placeholder' ) )
                  {
                    block.data( 'placeholder', block.text() );
                  }

                  block
                    .text( block.data( 'placeholder' ) );
                },
                success : function( response, text_status, xhr )
                {
                  var content_type = xhr.getResponseHeader( 'Content-Type' ) || '';
                  var highlight = null;

                  if( 0 === content_type.indexOf( 'text/xml' ) ||  0 === xhr.responseText.indexOf( '<?xml' ) ||
                      0 === content_type.indexOf( 'text/html' ) ||  0 === xhr.responseText.indexOf( '<!--' ) )
                  {
                    highlight = 'xml';
                  }
                  else if( 0 === content_type.indexOf( 'text/javascript' ) )
                  {
                    highlight = 'javascript';
                  }

                  var code = $(
                    '<pre class="syntax' + ( highlight ? ' language-' + highlight : '' )+ '"><code>' +
                    xhr.responseText.esc() +
                    '</code></pre>'
                  );
                  $( '.view-file .response', this )
                    .html( code );

                  if( highlight )
                  {
                    hljs.highlightBlock( code.get( 0 ) );
                  }

                  $( 'form textarea', this )
                    .val( xhr.responseText );
                },
                error : function( xhr, text_status, error_thrown)
                {
                  $( '.view-file .response', this )
                    .text( 'No such file exists.' );
                },
                complete : function( xhr, text_status )
                {
                }
              }
            );
          }
          load_file();
        }
      }
    );
  }
);

// legacy redirect for 'config' & 'schema' pages
// #/:core/schema, #/:core/config
sammy.get
(
  new RegExp( app.core_regex_base + '\\/(schema|config)$' ),
  function( context )
  {
    context.redirect( '#/' + context.params.splat[0] + '/files?file=' + this.active_core.attr( context.params.splat[1] ) );    
  }
);
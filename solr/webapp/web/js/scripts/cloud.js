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

// #/cloud
sammy.get
(
  /^#\/(cloud)$/,
  function( context )
  {
    var core_basepath = $( 'li[data-basepath]', app.menu_element ).attr( 'data-basepath' );
    var content_element = $( '#content' );

    $.get
    (
      'tpl/cloud.html',
      function( template )
      {
        content_element
          .html( template );

        var cloud_element = $( '#cloud', content_element );
        var cloud_content = $( '.content', cloud_element );

        var debug_element = $( '#debug', cloud_element );
        var debug_button = $( 'a.debug', cloud_element );

        var clipboard_element = $( '.clipboard', debug_element );
        var clipboard_button = $( 'a', clipboard_element );

        debug_button
          .die( 'click' )
          .live
          (
            'click',
            function( event )
            {
              debug_element.trigger( 'show' );
              return false;
            }
          );

        $( '.close', debug_element )
          .die( 'click' )
          .live
          (
            'click',
            function( event )
            {
              debug_element.trigger( 'hide' );
              return false;
            }
          );

        $( '.clipboard', debug_element )
          .die( 'click' )
          .live
          (
            'click',
            function( event )
            {
              return false;
            }
          );

        debug_element
          .die( 'show' )
          .live
          (
            'show',
            function( event )
            {
              debug_button.hide();
              debug_element.show();

              $.ajax
              (
                {
                  url : core_basepath + '/zookeeper?wt=json&dump=true',
                  dataType : 'text',
                  context : debug_element,
                  beforeSend : function( xhr, settings )
                  {
                    $( '.debug', debug_element )
                      .html( '<span class="loader">Loading Dump ...</span>' );

                    ZeroClipboard.setMoviePath( 'img/ZeroClipboard.swf' );

                    clipboard_client = new ZeroClipboard.Client();
                                    
                    clipboard_client.addEventListener
                    (
                      'load',
                      function( client )
                      {
                      }
                    );

                    clipboard_client.addEventListener
                    (
                      'complete',
                      function( client, text )
                      {
                        clipboard_element
                          .addClass( 'copied' );

                        clipboard_button
                          .data( 'text', clipboard_button.text() )
                          .text( clipboard_button.data( 'copied' ) );
                      }
                    );
                  },
                  success : function( response, text_status, xhr )
                  {
                    clipboard_client.glue
                    (
                      clipboard_element.get(0),
                      clipboard_button.get(0)
                    );

                    clipboard_client.setText( response.replace( /\\/g, '\\\\' ) );

                    $( '.debug', debug_element )
                      .removeClass( 'loader' )
                      .text( response );
                  },
                  error : function( xhr, text_status, error_thrown )
                  {
                  },
                  complete : function( xhr, text_status )
                  {
                  }
                }
              );
            }
          )
          .die( 'hide' )
          .live
          (
            'hide',
            function( event )
            {
              $( '.debug', debug_element )
                .empty();

              clipboard_element
                .removeClass( 'copied' );

              clipboard_button
                .data( 'copied', clipboard_button.text() )
                .text( clipboard_button.data( 'text' ) );

              clipboard_client.destroy();

              debug_button.show();
              debug_element.hide();
            }
          );

        $.ajax
        (
          {
            url : core_basepath + '/zookeeper?wt=json',
            dataType : 'json',
            context : cloud_content,
            beforeSend : function( xhr, settings )
            {
              //this
              //    .html( '<div class="loader">Loading ...</div>' );
            },
            success : function( response, text_status, xhr )
            {
              var self = this;
                            
              $( '#tree', this )
                .jstree
                (
                  {
                    "plugins" : [ "json_data" ],
                    "json_data" : {
                      "data" : response.tree,
                      "progressive_render" : true
                    },
                    "core" : {
                      "animation" : 0
                    }
                  }
                )
                .jstree
                (
                  'open_node',
                  'li:first'
                );

              var tree_links = $( '#tree a', this );

              tree_links
                .die( 'click' )
                .live
                (
                  'click',
                  function( event )
                  {
                    $( 'a.active', $( this ).parents( '#tree' ) )
                      .removeClass( 'active' );
                                        
                    $( this )
                      .addClass( 'active' );

                    cloud_content
                      .addClass( 'show' );

                    var file_content = $( '#file-content' );

                    $( 'a.close', file_content )
                      .die( 'click' )
                      .live
                      (
                        'click',
                        function( event )
                        {
                          $( '#tree a.active' )
                            .removeClass( 'active' );
                                            
                          cloud_content
                            .removeClass( 'show' );

                          return false;
                        }
                      );

                    $.ajax
                    (
                      {
                        url : this.href,
                        dataType : 'json',
                        context : file_content,
                        beforeSend : function( xhr, settings )
                        {
                          //this
                          //    .html( 'loading' )
                          //    .show();
                        },
                        success : function( response, text_status, xhr )
                        {
                          //this
                          //    .html( '<pre>' + response.znode.data + '</pre>' );

                          var props = [];
                          for( var key in response.znode.prop )
                          {
                            props.push
                            (
                              '<li><dl class="clearfix">' + "\n" +
                                '<dt>' + key.esc() + '</dt>' + "\n" +
                                '<dd>' + response.znode.prop[key].esc() + '</dd>' + "\n" +
                              '</dl></li>'
                            );
                          }

                          $( '#prop ul', this )
                            .empty()
                            .html( props.join( "\n" ) );

                          $( '#prop ul li:odd', this )
                            .addClass( 'odd' );

                          var data_element = $( '#data', this );

                          if( 0 !== parseInt( response.znode.prop.children_count ) )
                          {
                            data_element.hide();
                          }
                          else
                          {
                            var data = response.znode.data
                                     ? '<pre>' + response.znode.data.esc() + '</pre>'
                                     : '<em>File "' + response.znode.path + '" has no Content</em>';

                            data_element
                                .show()
                                .html( data );
                          }
                        },
                        error : function( xhr, text_status, error_thrown)
                        {
                        },
                        complete : function( xhr, text_status )
                        {
                        }
                      }
                    );

                    return false;
                  }
                );
            },
            error : function( xhr, text_status, error_thrown )
            {
              var message = 'Loading of <code>' + app.config.zookeeper_path + '</code> failed with "' + text_status + '" '
                          + '(<code>' + error_thrown.message + '</code>)';

              if( 200 !== xhr.status )
              {
                message = 'Loading of <code>' + app.config.zookeeper_path + '</code> failed with HTTP-Status ' + xhr.status + ' ';
              }

              this
                .html( '<div class="block" id="error">' + message + '</div>' );
            },
            complete : function( xhr, text_status )
            {
            }
          }
        );
      }
    );
  }
);
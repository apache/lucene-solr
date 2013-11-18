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
    var file_exists = null;

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

        var new_file_form = $( '#new-file-holder form' );

        $( '#new-file-holder > button' )
          .on
          (
            'click',
            function( event )
            {
              new_file_form.toggle();
              return false;
            }
          );

        new_file_form
          .on
          (
            'submit',
            function( event )
            {
              $( 'body' )
                .animate( { scrollTop: 0 }, 500 );

              window.location.href = '#/' + current_core + '/files?file=' + $( 'input', this ).val();

              return false;
            }
          );

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
          var public_url = window.location.protocol + '//' + window.location.host + endpoint;

          $( '#url', frame_element )
            .text( public_url )
            .attr( 'href', public_url );

          var form = $( 'form.modify', frame_element );

          form
            .attr( 'action', file_endpoint + '?wt=json&op=write&file=' + selected_file )
            .ajaxForm
            (
              {
                context : form,
                beforeSubmit: function( arr, form, options )
                {
                  $( 'button span', form )
                    .addClass( 'loader' );
                },
                success : function( response, status, xhr )
                {
                  $( 'button span', this )
                    .removeClass( 'loader' );

                  var button = $( 'button', this );

                  button
                    .addClass( 'success' );

                  load_file( !file_exists );

                  window.setTimeout
                  (
                    function()
                    {
                      button
                        .removeClass( 'success' );
                    },
                    1000
                  );
                }
              }
            );

          var change_button_label = function( form, label )
          {
            $( 'span[data-' + label + ']', form )
              .each
              (
                function( index, element )
                {
                  var self = $( this );
                  self.text( self.data( label ) );
                }
              );
          }

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
                context : $( 'form', frame_element ),
                beforeSend : function( xhr, settings )
                {
                },
                success : function( response, text_status, xhr )
                {
                  change_button_label( this, 'existing-title' );

                  $( 'textarea', this )
                    .val( xhr.responseText );
                },
                error : function( xhr, text_status, error_thrown)
                {
                  change_button_label( this, 'new-title' );
                },
                complete : function( xhr, text_status )
                {
                  file_exists = 200 === xhr.status;
                  $( '#new-file-note' )
                    .toggle( !file_exists );
                }
              }
            );
          }
          load_file();

          $( 'form.upload', frame_element )
            .on
            (
              'submit',
              function( event )
              {
                $( 'form input', frame_element )
                  .ajaxfileupload
                  (
                    {
                      action: endpoint + '&op=write&wt=json',
                      validate_extensions: false,
                      upload_now: true,
                      onStart: function ()
                      {
                        $( 'form.upload button span', frame_element )
                          .addClass( 'loader' );
                      },
                      onCancel: function ()
                      {
                        $( 'form.upload button span', frame_element )
                          .removeClass( 'loader' );
                      },
                      onComplete: function( response )
                      {
                        $( 'form.upload button span', frame_element )
                          .removeClass( 'loader' );

                        var button = $( 'form.upload button', frame_element );

                        button
                          .addClass( 'success' );

                        load_file( !file_exists );

                        $( 'body' )
                          .animate( { scrollTop: 0 }, 500 );

                        window.setTimeout
                        (
                          function()
                          {
                            button
                              .removeClass( 'success' );
                          },
                          1000
                        );
                      }
                    }
                  );

                return false;
              }
            );
        }
      }
    );
  }
);
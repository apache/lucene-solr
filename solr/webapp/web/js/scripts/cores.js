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
  'cores_load_data',
  function( event, params )
  {
    if( app.cores_data )
    {
      params.callback( app.cores_data );
      return true;
    }

    $.ajax
    (
      {
        url : app.config.solr_path + app.config.core_admin_path + '?wt=json',
        dataType : 'json',
        beforeSend : function( xhr, settings )
        {
        },
        success : function( response, text_status, xhr )
        {
          app.cores_data = response.status;
          params.callback( app.cores_data );
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

sammy.bind
(
  'cores_build_navigation',
  function( event, params )
  {
    var navigation_content = ['<ul>'];

    for( var core in params.cores )
    {
      var core_name = core;
      if( !core_name )
      {
        core_name = '<em>(empty)</em>';
      }
      navigation_content.push( '<li><a href="' + params.basepath + core + '">' + core_name + '</a></li>' );
    }

    params.navigation_element
      .html( navigation_content.join( "\n" ) );
        
    $( 'a[href="' + params.basepath + params.current_core + '"]', params.navigation_element ).parent()
      .addClass( 'current' );
  }
);

sammy.bind
(
  'cores_load_template',
  function( event, params )
  {
    if( app.cores_template )
    {
      params.callback();
      return true;
    }

    $.get
    (
      'tpl/cores.html',
      function( template )
      {
        params.content_element
          .html( template );
             
        app.cores_template = template;   
        params.callback();
      }
    );
  }
);

// #/~cores
sammy.get
(
  /^#\/~(cores)$/,
  function( context )
  {
    delete app.cores_template;

    sammy.trigger
    (
      'cores_load_data',
      {
        callback :  function( cores )
        {
          var first_core = null;
          for( var key in cores )
          {
            if( !first_core )
            {
              first_core = key;
            }
            continue;
          }
          context.redirect( context.path + '/' + first_core );
        }
      }
    );
  }
);

// #/~cores
sammy.get
(
  /^#\/~(cores)\//,
  function( context )
  {
    var content_element = $( '#content' );

    var path_parts = this.path.match( /^(.+\/~cores\/)(.*)$/ );
    var current_core = path_parts[2];

    sammy.trigger
    (
      'cores_load_data',
      {
        callback : function( cores )
        {
          sammy.trigger
          (
            'cores_load_template',
            {
              content_element : content_element,
              callback : function()
              {
                var cores_element = $( '#cores', content_element );
                var navigation_element = $( '#navigation', cores_element );
                var list_element = $( '#list', navigation_element );
                var data_element = $( '#data', cores_element );
                var core_data_element = $( '#core-data', data_element );
                var index_data_element = $( '#index-data', data_element );

                sammy.trigger
                (
                  'cores_build_navigation',
                  {
                    cores : cores,
                    basepath : path_parts[1],
                    current_core : current_core,
                    navigation_element : list_element
                  }
                );

                var core_data = cores[current_core];
                var core_basepath = $( '#' + current_core, app.menu_element ).attr( 'data-basepath' );

                // core-data

                $( 'h2 span', core_data_element )
                  .html( core_data.name );

                $( '.startTime dd', core_data_element )
                  .html( core_data.startTime );

                $( '.instanceDir dd', core_data_element )
                  .html( core_data.instanceDir );

                $( '.dataDir dd', core_data_element )
                  .html( core_data.dataDir );

                // index-data

                $( '.lastModified dd', index_data_element )
                  .html( core_data.index.lastModified );

                $( '.version dd', index_data_element )
                  .html( core_data.index.version );

                $( '.numDocs dd', index_data_element )
                  .html( core_data.index.numDocs );

                $( '.maxDoc dd', index_data_element )
                  .html( core_data.index.maxDoc );

                $( '.optimized dd', index_data_element )
                  .addClass( core_data.index.optimized ? 'ico-1' : 'ico-0' );

                $( '#actions .optimize', cores_element )
                  .show();

                $( '.optimized dd span', index_data_element )
                  .html( core_data.index.optimized ? 'yes' : 'no' );

                $( '.current dd', index_data_element )
                  .addClass( core_data.index.current ? 'ico-1' : 'ico-0' );

                $( '.current dd span', index_data_element )
                  .html( core_data.index.current ? 'yes' : 'no' );

                $( '.hasDeletions dd', index_data_element )
                  .addClass( core_data.index.hasDeletions ? 'ico-1' : 'ico-0' );

                $( '.hasDeletions dd span', index_data_element )
                  .html( core_data.index.hasDeletions ? 'yes' : 'no' );

                $( '.directory dd', index_data_element )
                  .html
                  (
                    core_data.index.directory
                      .replace( /:/g, ':&#8203;' )
                      .replace( /@/g, '@&#8203;' )
                  );

                var core_names = [];
                var core_selects = $( '#actions select', cores_element );

                for( var key in cores )
                {
                  core_names.push( '<option value="' + key + '">' + key + '</option>' )
                }

                core_selects
                  .html( core_names.join( "\n") );
                                
                $( 'option[value="' + current_core + '"]', core_selects.filter( '#swap_core' ) )
                  .attr( 'selected', 'selected' );

                $( 'option[value="' + current_core + '"]', core_selects.filter( '.other' ) )
                  .attr( 'disabled', 'disabled' )
                  .addClass( 'disabled' );
                                
                $( 'input[name="core"]', cores_element )
                  .val( current_core );

                // layout

                var actions_element = $( '.actions', cores_element );
                var button_holder_element = $( '.button-holder.options', actions_element );

                button_holder_element
                  .die( 'toggle' )
                  .live
                  (
                    'toggle',
                    function( event )
                    {
                      var element = $( this );
                                        
                      element
                        .toggleClass( 'active' );
                                            
                      if( element.hasClass( 'active' ) )
                      {
                        button_holder_element
                          .not( element )
                          .removeClass( 'active' );
                      }
                    }
                  );

                $( '.button a', button_holder_element )
                  .die( 'click' )
                  .live
                  (
                    'click',
                    function( event )
                    {
                      $( this ).parents( '.button-holder' )
                        .trigger( 'toggle' );
                    }
                  );

                $( 'form a.submit', button_holder_element )
                  .die( 'click' )
                  .live
                  (
                    'click',
                    function( event )
                    {
                      var element = $( this );
                      var form_element = element.parents( 'form' );
                      var action = $( 'input[name="action"]', form_element ).val().toLowerCase();

                      form_element
                        .ajaxSubmit
                        (
                          {
                            url : app.config.solr_path + app.config.core_admin_path + '?wt=json',
                            dataType : 'json',
                            beforeSubmit : function( array, form, options )
                            {
                              //loader
                            },
                            success : function( response, status_text, xhr, form )
                            {
                              delete app.cores_data;

                              if( 'rename' === action )
                              {
                                context.redirect( path_parts[1] + $( 'input[name="other"]', form_element ).val() );
                              }
                              else if( 'swap' === action )
                              {
                                window.location.reload();
                              }
                                                            
                              $( 'a.reset', form )
                                .trigger( 'click' );
                            },
                            error : function( xhr, text_status, error_thrown )
                            {
                            },
                            complete : function()
                            {
                              //loader
                            }
                          }
                        );

                      return false;
                    }
                  );

                $( 'form a.reset', button_holder_element )
                  .die( 'click' )
                  .live
                  (
                    'click',
                    function( event )
                    {
                      $( this ).parents( 'form' )
                        .resetForm();

                      $( this ).parents( '.button-holder' )
                        .trigger( 'toggle' );
                                            
                      return false;
                    }
                  );

                var reload_button = $( '#actions .reload', cores_element );
                reload_button
                  .die( 'click' )
                  .live
                  (
                    'click',
                    function( event )
                    {
                      $.ajax
                      (
                        {
                          url : app.config.solr_path + app.config.core_admin_path + '?wt=json&action=RELOAD&core=' + current_core,
                          dataType : 'json',
                          context : $( this ),
                          beforeSend : function( xhr, settings )
                          {
                            this
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
                                reload_button
                                  .removeClass( 'success' );
                              },
                              5000
                            );
                          },
                          error : function( xhr, text_status, error_thrown )
                          {
                          },
                          complete : function( xhr, text_status )
                          {
                            this
                              .removeClass( 'loader' );
                          }
                        }
                      );
                    }
                  );
                                
                $( '#actions .unload', cores_element )
                  .die( 'click' )
                  .live
                  (
                    'click',
                    function( event )
                    {
                      $.ajax
                      (
                        {
                          url : app.config.solr_path + app.config.core_admin_path + '?wt=json&action=UNLOAD&core=' + current_core,
                          dataType : 'json',
                          context : $( this ),
                          beforeSend : function( xhr, settings )
                          {
                            this
                              .addClass( 'loader' );
                          },
                          success : function( response, text_status, xhr )
                          {
                            delete app.cores_data;
                            context.redirect( path_parts[1].substr( 0, path_parts[1].length - 1 ) );
                          },
                          error : function( xhr, text_status, error_thrown )
                          {
                          },
                          complete : function( xhr, text_status )
                          {
                            this
                              .removeClass( 'loader' );
                          }
                        }
                      );
                    }
                  );

                var optimize_button = $( '#actions .optimize', cores_element );
                optimize_button
                  .die( 'click' )
                  .live
                  (
                    'click',
                    function( event )
                    {
                      $.ajax
                      (
                        {
                          url : core_basepath + '/update?optimize=true&waitFlush=true&wt=json',
                          dataType : 'json',
                          context : $( this ),
                          beforeSend : function( xhr, settings )
                          {
                            this
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
                                optimize_button
                                  .removeClass( 'success' );
                              },
                              5000
                            );
                                                        
                            $( '.optimized dd.ico-0', index_data_element )
                              .removeClass( 'ico-0' )
                              .addClass( 'ico-1' );
                          },
                          error : function( xhr, text_status, error_thrown)
                          {
                            console.warn( 'd0h, optimize broken!' );
                          },
                          complete : function( xhr, text_status )
                          {
                            this
                              .removeClass( 'loader' );
                          }
                        }
                      );
                    }
                  );

                $( '.timeago', data_element )
                  .timeago();

                $( 'ul', data_element )
                  .each
                  (
                    function( i, element )
                    {
                      $( 'li:odd', element )
                        .addClass( 'odd' );
                    }
                  )
              }
            }
          );
        }
      }
    );
  }
);
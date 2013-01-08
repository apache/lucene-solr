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

// #/:core/query
sammy.get
(
  new RegExp( app.core_regex_base + '\\/(query)$' ),
  function( context )
  {
    var core_basepath = this.active_core.attr( 'data-basepath' );
    var content_element = $( '#content' );
        
    $.get
    (
      'tpl/query.html',
      function( template )
      {
        content_element
          .html( template );

        var query_element = $( '#query', content_element );
        var query_form = $( '#form form', query_element );
        var url_element = $( '#url', query_element );
        var result_element = $( '#result', query_element );
        var response_element = $( '#response', result_element );

        url_element
          .die( 'change' )
          .live
          (
            'change',
            function( event )
            {
              var wt = $( '[name="wt"]', query_form ).val();

              var content_generator = {

                _default : function( xhr )
                {
                  return xhr.responseText.esc();
                },

                json : function( xhr )
                {
                  return JSON.stringify( JSON.parse( xhr.responseText ), undefined, 2 );
                }

              };

              $.ajax
              (
                {
                  url : this.href,
                  dataType : wt,
                  context : response_element,
                  beforeSend : function( xhr, settings )
                  {
                    this
                     .html( '<div class="loader">Loading ...</div>' );
                  },
                  complete : function( xhr, text_status )
                  {
                    var code = $(
                      '<pre class="syntax language-' + wt + '"><code>' +
                      ( content_generator[wt] || content_generator['_default'] )( xhr ) +
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
          )

        $( '.optional legend input[type=checkbox]', query_form )
          .die( 'change' )
          .live
          (
            'change',
            function( event )
            {
              var fieldset = $( this ).parents( 'fieldset' );

              this.checked
                ? fieldset.addClass( 'expanded' )
                : fieldset.removeClass( 'expanded' );
            }
          );

        query_form
          .die( 'submit' )
          .live
          (
            'submit',
            function( event )
            {
              var form_map = {};
              var form_values = [];
              var all_form_values = query_form.formToArray();

              for( var i = 0; i < all_form_values.length; i++ )
              {
                if( !all_form_values[i].value || 0 === all_form_values[i].value.length )
                {
                  continue;
                }

                var name_parts = all_form_values[i].name.split( '.' );
                if( 1 < name_parts.length && !form_map[name_parts[0]] )
                {
                  console.debug( 'skip "' + all_form_values[i].name + '", parent missing' );
                  continue;
                }

                form_map[all_form_values[i].name] = all_form_values[i].value;
                form_values.push( all_form_values[i] );
              }

              var handler_path = $( '#qt', query_form ).val();
              if( '/' !== handler_path[0] )
              {
                form_values.push( { name : 'qt', value : handler_path.esc() } );
                handler_path = '/select';
              }

              var query_url = window.location.protocol + '//' + window.location.host
                            + core_basepath + handler_path + '?' + $.param( form_values );
                            
              url_element
                .attr( 'href', query_url )
                .text( query_url )
                .trigger( 'change' );
                            
              result_element
                .show();
                            
              return false;
            }
          );

        var fields = 0;
        for( var key in context.params )
        {
          if( 'string' === typeof context.params[key] )
          {
            fields++;
            $( '[name="' + key + '"]', query_form )
              .val( context.params[key] );
          }
        }

        if( 0 !== fields )
        {
          query_form
            .trigger( 'submit' );
        }
      }
    );
  }
);
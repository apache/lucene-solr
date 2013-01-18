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
                  return app.format_json( xhr.responseText );
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
              var form_values = [];
 
              var add_to_form_values = function add_to_form_values( fields )
              {
                 for( var i in fields )
                 {
                  if( !fields[i].value || 0 === fields[i].value.length )
                  {
                    continue;
                  }
 
                  form_values.push( fields[i] );
                 }
              };
 
              var fieldsets = $( '> fieldset', query_form );
 
              var fields = fieldsets.first().formToArray();
              add_to_form_values( fields );

              fieldsets.not( '.common' )
                .each
                (
                  function( i, set )
                  {
                    if( $( 'legend input', set ).is( ':checked' ) )
                    {
                      var fields = $( set ).formToArray();
                      add_to_form_values( fields );
                    }
                  }
                );

              var handler_path = $( '#qt', query_form ).val();
              if( '/' !== handler_path[0] )
              {
                form_values.push( { name : 'qt', value : handler_path.esc() } );
                handler_path = '/select';
              }

              var query_url = window.location.protocol + '//' + window.location.host
                            + core_basepath + handler_path + '?' + $.param( form_values );

              var custom_parameters = $( '#custom_parameters', query_form ).val();
              if( custom_parameters && 0 !== custom_parameters.length )
              {
                query_url += '&' + custom_parameters.replace( /^&/, '' ); 
              }

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
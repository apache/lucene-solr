// #/:core/query
sammy.get
(
    /^#\/([\w\d-]+)\/(query)$/,
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
                var response_element = $( '#response iframe', result_element );

                url_element
                    .die( 'change' )
                    .live
                    (
                        'change',
                        function( event )
                        {
                            var check_iframe_ready_state = function()
                            {
                                var iframe_element = response_element.get(0).contentWindow.document || 
                                                     response_element.get(0).document;

                                if( !iframe_element )
                                {
                                    console.debug( 'no iframe_element found', response_element );
                                    return false;
                                }

                                url_element
                                    .addClass( 'loader' );

                                if( 'complete' === iframe_element.readyState )
                                {
                                    url_element
                                        .removeClass( 'loader' );
                                }
                                else
                                {
                                    window.setTimeout( check_iframe_ready_state, 100 );
                                }
                            }
                            check_iframe_ready_state();

                            response_element
                                .attr( 'src', this.href );
                            
                            if( !response_element.hasClass( 'resized' ) )
                            {
                                response_element
                                    .addClass( 'resized' )
                                    .css( 'height', $( '#main' ).height() - 60 );
                            }
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
                    )

                for( var key in context.params )
                {
                    if( 'string' === typeof context.params[key] )
                    {
                        $( '[name="' + key + '"]', query_form )
                            .val( context.params[key] );
                    }
                }

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

                            var query_url = window.location.protocol + '//' +
                                            window.location.host +
                                            core_basepath +
                                            '/select?' +
                                            $.param( form_values );
                            
                            url_element
                                .attr( 'href', query_url )
                                .text( query_url )
                                .trigger( 'change' );
                            
                            result_element
                                .show();
                            
                            return false;
                        }
                    );
            }
        );
    }
);
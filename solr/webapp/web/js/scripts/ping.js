$( '.ping a', app.menu_element )
    .live
    (
        'click',
        function( event )
        {
            $.ajax
            (
                {
                    url : $( this ).attr( 'rel' ) + '?wt=json&ts=' + (new Date).getTime(),
                    dataType : 'json',
                    context: this,
                    beforeSend : function( arr, form, options )
                    {
                        loader.show( this );
                    },
                    success : function( response, text_status, xhr )
                    {
                        $( this )
                            .removeAttr( 'title' );
                        
                        $( this ).parents( 'li' )
                            .removeClass( 'error' );
                            
                        var qtime_element = $( '.qtime', this );
                        
                        if( 0 === qtime_element.size() )
                        {
                            qtime_element = $( '<small class="qtime"> (<span></span>)</small>' );
                            
                            $( this )
                                .append
                                (
                                    qtime_element
                                );
                        }
                        
                        $( 'span', qtime_element )
                            .html( response.responseHeader.QTime + 'ms' );
                    },
                    error : function( xhr, text_status, error_thrown )
                    {
                        $( this )
                            .attr( 'title', '/admin/ping is not configured (' + xhr.status + ': ' + error_thrown + ')' );
                        
                        $( this ).parents( 'li' )
                            .addClass( 'error' );
                    },
                    complete : function( xhr, text_status )
                    {
                        loader.hide( this );
                    }
                }
            );
            
            return false;
        }
    );
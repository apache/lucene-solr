// #/logging
sammy.get
(
    /^#\/(logging)$/,
    function( context )
    {
        var content_element = $( '#content' );
        
        content_element
            .html( '<div id="logging"></div>' );

        $.ajax
        (
            {
                url : 'logging.json',
                dataType : 'json',
                context : $( '#logging', content_element ),
                beforeSend : function( xhr, settings )
                {
                    this
                        .html( '<div class="loader">Loading ...</div>' );
                },
                success : function( response, text_status, xhr )
                {
                    var logger = response.logger;

                    var loglevel = '<div class="loglevel %class%">' + "\n";
                    loglevel += '<a class="effective_level trigger"><span>%effective_level%</span></a>' + "\n";
                    loglevel += '<ul>' + "\n";

                    for( var key in response.levels )
                    {
                        var level = response.levels[key].esc();
                        loglevel += '<li class="' + level + '"><a>' + level + '</a></li>' + "\n";
                    }

                    loglevel += '<li class="UNSET"><a>UNSET</a></li>' + "\n";
                    loglevel += '</ul>' + "\n";
                    loglevel += '</div>';

                    var logger_tree = function( filter )
                    {
                        var logger_content = '';
                        var filter_regex = new RegExp( '^' + filter + '\\.\\w+$' );

                        for( var logger_name in logger )
                        {
                            var continue_matcher = false;

                            if( !filter )
                            {
                                continue_matcher = logger_name.indexOf( '.' ) !== -1;
                            }
                            else
                            {
                                continue_matcher = !logger_name.match( filter_regex );
                            }

                            if( continue_matcher )
                            {
                                continue;
                            }

                            var has_logger_instance = !!logger[logger_name];

                            var classes = [];

                            has_logger_instance
                                ? classes.push( 'active' )
                                : classes.push( 'inactive' );

                            logger_content += '<li class="jstree-leaf">';
                            logger_content += '<ins class="jstree-icon">&nbsp;</ins>';
                            logger_content += '<a class="trigger ' + classes.join( ' ' ) + '" ' + "\n" +
                                                 'title="' + logger_name.esc() + '"><span>' + "\n" +
                                                logger_name.split( '.' ).pop().esc() + "\n" +
                                              '</span></a>';

                            logger_content += loglevel
                                                .replace
                                                (
                                                    /%class%/g,
                                                    classes.join( ' ' )
                                                )
                                                .replace
                                                (
                                                    /%effective_level%/g,
                                                    has_logger_instance
                                                        ? logger[logger_name].effective_level
                                                        : 'null'
                                                );

                            var child_logger_content = logger_tree( logger_name );
                            if( child_logger_content )
                            {
                                logger_content += '<ul>';
                                logger_content += child_logger_content;
                                logger_content += '</ul>';
                            }

                            logger_content += '</li>';
                        }

                        return logger_content;
                    }

                    var logger_content = logger_tree( null );
                    
                    var warn = '<div>TODO, this is not yet implemented.  For now, use <a href="logging" style="color:00AA00;">the old logging UI</a></div><br/>'


                    this.html( warn + '<ul class="tree jstree">' + logger_content + '</ul>' );

                    $( 'li:last-child', this )
                        .addClass( 'jstree-last' );
                    
                    $( '.loglevel', this )
                        .each
                        (
                            function( index, element )
                            {
                                var element = $( element );
                                var effective_level = $( '.effective_level span', element ).text();

                                element
                                    .css( 'z-index', 800 - index );
                                
                                $( 'ul .' + effective_level, element )
                                    .addClass( 'selected' );
                            }
                        );

                    $( '.trigger', this )
                        .die( 'click' )
                        .live
                        (
                            'click',
                            function( event )
                            {
                                $( '.loglevel', $( this ).parents( 'li' ).first() ).first()
                                    .trigger( 'toggle' );
                            }
                        );
                    
                    $( '.loglevel', this )
                        .die( 'toggle')
                        .live
                        (
                            'toggle',
                            function( event )
                            {
                                $( this )
                                    .toggleClass( 'open' );
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
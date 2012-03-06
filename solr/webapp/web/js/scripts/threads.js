// #/threads
sammy.get
(
    /^#\/(threads)$/,
    function( context )
    {
        var core_basepath = $( 'li[data-basepath]', app.menu_element ).attr( 'data-basepath' );
        var content_element = $( '#content' );

        $.get
        (
            'tpl/threads.html',
            function( template )
            {
                content_element
                    .html( template );

                $.ajax
                (
                    {
                        url : core_basepath + '/admin/threads?wt=json',
                        dataType : 'json',
                        context : $( '#threads', content_element ),
                        beforeSend : function( xhr, settings )
                        {
                        },
                        success : function( response, text_status, xhr )
                        {
                            var self = this;

                            var threadDumpData = response.system.threadDump;
                            var threadDumpContent = [];
                            var c = 0;
                            for( var i = 1; i < threadDumpData.length; i += 2 )
                            {
                                var state = threadDumpData[i].state.esc();
                                var name = '<a title="' + state +'"><span>' + threadDumpData[i].name.esc() + ' (' + threadDumpData[i].id.esc() + ')</span></a>';

                                var classes = [state];
                                var details = '';

                                if( 0 !== c % 2 )
                                {
                                    classes.push( 'odd' );
                                }

                                if( threadDumpData[i].lock )
                                {
                                    classes.push( 'lock' );
                                    name += "\n" + '<p title="Waiting on">' + threadDumpData[i].lock.esc() + '</p>';
                                }

                                if( threadDumpData[i].stackTrace && 0 !== threadDumpData[i].stackTrace.length )
                                {
                                    classes.push( 'stacktrace' );

                                    var stack_trace = threadDumpData[i].stackTrace
                                                        .join( '###' )
                                                        .esc()
                                                        .replace( /\(/g, '&#8203;(' )
                                                        .replace( /###/g, '</li><li>' );

                                    name += '<div>' + "\n"
                                            + '<ul>' + "\n"
                                            + '<li>' + stack_trace + '</li>'
                                            + '</ul>' + "\n"
                                            + '</div>';
                                }

                                var item = '<tr class="' + classes.join( ' ' ) +'">' + "\n"

                                         + '<td class="name">' + name + '</td>' + "\n"
                                         + '<td class="time">' + threadDumpData[i].cpuTime.esc() + '<br>' + threadDumpData[i].userTime.esc() + '</td>' + "\n"

                                         + '</tr>';
                                
                                threadDumpContent.push( item );
                                c++;
                            }

                            var threadDumpBody = $( '#thread-dump tbody', this );

                            threadDumpBody
                                .html( threadDumpContent.join( "\n" ) );
                            
                            $( '.name a', threadDumpBody )
                                .die( 'click' )
                                .live
                                (
                                    'click',
                                    function( event )
                                    {
                                        $( this ).closest( 'tr' )
                                            .toggleClass( 'open' );
                                    }
                                );
                            
                            $( '.controls a', this )
                                .die( 'click' )
                                .live
                                (
                                    'click',
                                    function( event )
                                    {
                                        var threads_element = $( self );
                                        var is_collapsed = threads_element.hasClass( 'collapsed' );
                                        var thread_rows = $( 'tr', threads_element );

                                        thread_rows
                                            .each
                                            (
                                                function( index, element )
                                                {
                                                    if( is_collapsed )
                                                    {
                                                        $( element )
                                                            .addClass( 'open' );
                                                    }
                                                    else
                                                    {
                                                        $( element )
                                                            .removeClass( 'open' );
                                                    }
                                                }
                                            );

                                        threads_element
                                            .toggleClass( 'collapsed' )
                                            .toggleClass( 'expanded' );
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
    }
);
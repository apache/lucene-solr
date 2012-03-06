// #/
sammy.get
(
    /^#\/$/,
    function( context )
    {
        var content_element = $( '#content' );

        $( '#index', app.menu_element )
            .addClass( 'active' );

        content_element
            .html( '<div id="index"></div>' );

        $.ajax
        (
            {
                url : 'tpl/index.html',
                context : $( '#index', content_element ),
                beforeSend : function( arr, form, options )
                {
                },
                success : function( template )
                {
                    this
                        .html( template );

                    var jvm_memory = $.extend
                    (
                        {
                            'free' : null,
                            'total' : null,
                            'max' : null,
                            'used' : null,
                            'raw' : {
                                'free' : null,
                                'total' : null,
                                'max' : null,
                                'used' : null,
                                'used%' : null
                            }
                        },
                        app.dashboard_values['jvm']['memory']
                    );

                    var parse_memory_value = function( value )
                    {
                        if( value !== Number( value ) )
                        {
                            var units = 'BKMGTPEZY';
                            var match = value.match( /^(\d+([,\.]\d+)?) (\w)\w?$/ );
                            var value = parseFloat( match[1] ) * Math.pow( 1024, units.indexOf( match[3].toUpperCase() ) );
                        }
                        
                        return value;
                    };
                    var memory_data = {
                        'memory-bar-max' : parse_memory_value( jvm_memory['raw']['max'] || jvm_memory['max'] ),
                        'memory-bar-total' : parse_memory_value( jvm_memory['raw']['total'] || jvm_memory['total'] ),
                        'memory-bar-used' : parse_memory_value( jvm_memory['raw']['used'] || jvm_memory['used'] )
                    };                            
    
                    for( var key in memory_data )
                    {                                                        
                        $( '.value.' + key, this )
                            .text( memory_data[key] );
                    }
    
                    var data = {
                        'start_time' : app.dashboard_values['jvm']['jmx']['startTime'],
                        'host' : app.dashboard_values['core']['host'],
                        'jvm' : app.dashboard_values['jvm']['name'] + ' (' + app.dashboard_values['jvm']['version'] + ')',
                        'solr_spec_version' : app.dashboard_values['lucene']['solr-spec-version'],
                        'solr_impl_version' : app.dashboard_values['lucene']['solr-impl-version'],
                        'lucene_spec_version' : app.dashboard_values['lucene']['lucene-spec-version'],
                        'lucene_impl_version' : app.dashboard_values['lucene']['lucene-impl-version']
                    };

                    if( app.dashboard_values['core']['directory']['cwd'] )
                    {
                        data['cwd'] = app.dashboard_values['core']['directory']['cwd'];
                    }
    
                    for( var key in data )
                    {                                                        
                        var value_element = $( '.' + key + ' dd', this );

                        value_element
                            .text( data[key] );
                        
                        value_element.closest( 'li' )
                            .show();
                    }

                    var commandLineArgs = app.dashboard_values['jvm']['jmx']['commandLineArgs'];
                    if( 0 !== commandLineArgs.length )
                    {
                        var cmd_arg_element = $( '.command_line_args dt', this );
                        var cmd_arg_key_element = $( '.command_line_args dt', this );
                        var cmd_arg_element = $( '.command_line_args dd', this );

                        for( var key in commandLineArgs )
                        {
                            cmd_arg_element = cmd_arg_element.clone();
                            cmd_arg_element.text( commandLineArgs[key] );

                            cmd_arg_key_element
                                .after( cmd_arg_element );
                        }

                        cmd_arg_key_element.closest( 'li' )
                            .show();

                        $( '.command_line_args dd:last', this )
                            .remove();

                        $( '.command_line_args dd:odd', this )
                            .addClass( 'odd' );
                    }

                    $( '.timeago', this )
                        .timeago();

                    $( 'li:visible:odd', this )
                        .addClass( 'odd' );
                    
                    // -- memory bar

                    var max_height = Math.round( $( '#memory-bar-max', this ).height() );
                    var total_height = Math.round( ( memory_data['memory-bar-total'] * max_height ) / memory_data['memory-bar-max'] );
                    var used_height = Math.round( ( memory_data['memory-bar-used'] * max_height ) / memory_data['memory-bar-max'] );

                    var memory_bar_total_value = $( '#memory-bar-total span', this ).first();

                    $( '#memory-bar-total', this )
                        .height( total_height );
                    
                    $( '#memory-bar-used', this )
                        .height( used_height );

                    if( used_height < total_height + memory_bar_total_value.height() )
                    {
                        memory_bar_total_value
                            .addClass( 'upper' )
                            .css( 'margin-top', memory_bar_total_value.height() * -1 );
                    }

                    var memory_percentage = ( ( memory_data['memory-bar-used'] / memory_data['memory-bar-max'] ) * 100 ).toFixed(1);
                    var headline = $( '#memory h2 span', this );
                        
                    headline
                        .text( headline.html() + ' (' + memory_percentage + '%)' );

                    $( '#memory-bar .value', this )
                        .each
                        (
                            function()
                            {
                                var self = $( this );

                                var byte_value = parseInt( self.html() );

                                self
                                    .attr( 'title', 'raw: ' + byte_value + ' B' );

                                byte_value /= 1024;
                                byte_value /= 1024;
                                byte_value = byte_value.toFixed( 2 ) + ' MB';

                                self
                                    .text( byte_value );
                            }
                        );
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
);
// #/cloud
sammy.get
(
    /^#\/(cloud)$/,
    function( context )
    {
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

                $.ajax
                (
                    {
                        url : app.config.zookeeper_path,
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
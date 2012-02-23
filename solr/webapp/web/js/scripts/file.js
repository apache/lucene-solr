// #/:core/schema, #/:core/config
sammy.get
(
    /^#\/([\w\d-]+)\/(schema|config)$/,
    function( context )
    {
        var core_basepath = this.active_core.attr( 'data-basepath' );

        $.ajax
        (
            {
                url : core_basepath + app.config[ context.params.splat[1] + '_path' ],
                dataType : 'xml',
                context : $( '#content' ),
                beforeSend : function( xhr, settings )
                {
                    this
                        .html( '<div class="loader">Loading ...</div>' );
                },
                complete : function( xhr, text_status )
                {
                    var code = $(
                        '<pre class="syntax language-xml"><code>' +
                        xhr.responseText.esc() +
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
);
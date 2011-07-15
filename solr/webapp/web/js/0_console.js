if( 'undefined' === typeof( console ) )
{
    var console = {
        log : function() {},
        debug : function() {},
        dump : function() {},
        error : function() {},
        warn : function(){}
    };
}
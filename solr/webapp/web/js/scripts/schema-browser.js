sammy.bind
(
    'schema_browser_navi',
    function( event, params )
    {
        var related_navigation_element = $( '#related dl#f-df-t', params.schema_browser_element );
        var related_navigation_meta = $( '#related dl.ukf-dsf', params.schema_browser_element );
        var related_select_element = $( '#related select', params.schema_browser_element )
        var type = 'index';

        var sammy_basepath = '#/' + $( 'p a', params.active_core ).html() + '/schema-browser';
        
        if( !related_navigation_meta.hasClass( 'done' ) )
        {
            if( app.schema_browser_data.unique_key_field )
            {
                $( '.unique-key-field', related_navigation_meta )
                    .show()
                    .after
                    (
                        '<dd class="unique-key-field"><a href="' + sammy_basepath + '/field/' +
                        app.schema_browser_data.unique_key_field + '">' +
                        app.schema_browser_data.unique_key_field + '</a></dd>'
                    );
            }

            if( app.schema_browser_data.default_search_field )
            {
                $( '.default-search-field', related_navigation_meta )
                    .show()
                    .after
                    (
                        '<dd class="default-search-field"><a href="' + sammy_basepath + '/field/' +
                        app.schema_browser_data.default_search_field + '">' +
                        app.schema_browser_data.default_search_field + '</a></dd>'
                    );
            }

            related_navigation_meta
                .addClass( 'done' );
        }

        if( params.route_params )
        {
            var type = params.route_params.splat[3];
            var value = params.route_params.splat[4];

            var navigation_data = {
                'fields' : [],
                'copyfield_source' : [],
                'copyfield_dest' : [],
                'dynamic_fields' : [],
                'types' : []
            }

            $( 'option[value="' + params.route_params.splat[2] + '"]', related_select_element )
                .attr( 'selected', 'selected' );

            if( 'field' === type )
            {
                navigation_data.fields.push( value );
                navigation_data.types.push( app.schema_browser_data.relations.f_t[value] );

                if( app.schema_browser_data.relations.f_df[value] )
                {
                    navigation_data.dynamic_fields.push( app.schema_browser_data.relations.f_df[value] );
                }

                if( 0 !== app.schema_browser_data.fields[value].copySources.length )
                {
                    navigation_data.copyfield_source = app.schema_browser_data.fields[value].copySources;
                }

                if( 0 !== app.schema_browser_data.fields[value].copyDests.length )
                {
                    navigation_data.copyfield_dest = app.schema_browser_data.fields[value].copyDests;
                }
            }
            else if( 'dynamic-field' === type )
            {
                navigation_data.dynamic_fields.push( value );
                navigation_data.types.push( app.schema_browser_data.relations.df_t[value] );

                if( app.schema_browser_data.relations.df_f[value] )
                {
                    navigation_data.fields = app.schema_browser_data.relations.df_f[value];
                }
            }
            else if( 'type' === type )
            {
                navigation_data.types.push( value );
                
                if( app.schema_browser_data.relations.t_f[value] )
                {
                    navigation_data.fields = app.schema_browser_data.relations.t_f[value];
                }
                
                if( app.schema_browser_data.relations.t_df[value] )
                {
                    navigation_data.dynamic_fields = app.schema_browser_data.relations.t_df[value];
                }
            }

            var navigation_content = '';

            if( 0 !== navigation_data.fields.length )
            {
                navigation_data.fields.sort();
                navigation_content += '<dt class="field">Fields</dt>' + "\n";
                for( var i in navigation_data.fields )
                {
                    var href = sammy_basepath + '/field/' + navigation_data.fields[i];
                    navigation_content += '<dd class="field"><a href="' + href + '">' + 
                                          navigation_data.fields[i] + '</a></dd>' + "\n";
                }
            }

            if( 0 !== navigation_data.copyfield_source.length )
            {
                navigation_data.copyfield_source.sort();
                navigation_content += '<dt class="copyfield">Copied from</dt>' + "\n";
                for( var i in navigation_data.copyfield_source )
                {
                    var href = sammy_basepath + '/field/' + navigation_data.copyfield_source[i];
                    navigation_content += '<dd class="copyfield"><a href="' + href + '">' + 
                                          navigation_data.copyfield_source[i] + '</a></dd>' + "\n";
                }
            }

            if( 0 !== navigation_data.copyfield_dest.length )
            {
                navigation_data.copyfield_dest.sort();
                navigation_content += '<dt class="copyfield">Copied to</dt>' + "\n";
                for( var i in navigation_data.copyfield_dest )
                {
                    var href = sammy_basepath + '/field/' + navigation_data.copyfield_dest[i];
                    navigation_content += '<dd class="copyfield"><a href="' + href + '">' + 
                                          navigation_data.copyfield_dest[i] + '</a></dd>' + "\n";
                }
            }

            if( 0 !== navigation_data.dynamic_fields.length )
            {
                navigation_data.dynamic_fields.sort();
                navigation_content += '<dt class="dynamic-field">Dynamic Fields</dt>' + "\n";
                for( var i in navigation_data.dynamic_fields )
                {
                    var href = sammy_basepath + '/dynamic-field/' + navigation_data.dynamic_fields[i];
                    navigation_content += '<dd class="dynamic-field"><a href="' + href + '">' + 
                                          navigation_data.dynamic_fields[i] + '</a></dd>' + "\n";
                }
            }

            if( 0 !== navigation_data.types.length )
            {
                navigation_data.types.sort();
                navigation_content += '<dt class="type">Types</dt>' + "\n";
                for( var i in navigation_data.types )
                {
                    var href = sammy_basepath + '/type/' + navigation_data.types[i];
                    navigation_content += '<dd class="type"><a href="' + href + '">' + 
                                          navigation_data.types[i] + '</a></dd>' + "\n";
                }
            }

            related_navigation_element
                .show()
                .attr( 'class', type )
                .html( navigation_content );
        }
        else
        {
            related_navigation_element
                .hide();
            
            $( 'option:selected', related_select_element )
                .removeAttr( 'selected' );
        }

        if( 'field' === type && value === app.schema_browser_data.unique_key_field )
        {
            $( '.unique-key-field', related_navigation_meta )
                .addClass( 'active' );
        }
        else
        {
            $( '.unique-key-field', related_navigation_meta )
                .removeClass( 'active' );
        }

        if( 'field' === type && value === app.schema_browser_data.default_search_field )
        {
            $( '.default-search-field', related_navigation_meta )
                .addClass( 'active' );
        }
        else
        {
            $( '.default-search-field', related_navigation_meta )
                .removeClass( 'active' );
        }

        if( params.callback )
        {
            params.callback( app.schema_browser_data, $( '#data', params.schema_browser_element ) );
        }
    }
);

sammy.bind
(
    'schema_browser_load',
    function( event, params )
    {
        var core_basepath = params.active_core.attr( 'data-basepath' );
        var content_element = $( '#content' );

        if( app.schema_browser_data )
        {
            params.schema_browser_element = $( '#schema-browser', content_element );

            sammy.trigger
            (
                'schema_browser_navi',
                params
            );
        }
        else
        {
            content_element
                .html( '<div id="schema-browser"><div class="loader">Loading ...</div></div>' );
            
            $.ajax
            (
                {
                    url : core_basepath + '/admin/luke?numTerms=0&wt=json',
                    dataType : 'json',
                    beforeSend : function( xhr, settings )
                    {
                    },
                    success : function( response, text_status, xhr )
                    {
                        app.schema_browser_data = {
                            default_search_field : null,
                            unique_key_field : null,
                            key : {},
                            fields : {},
                            dynamic_fields : {},
                            types : {},
                            relations : {
                                f_df : {},
                                f_t  : {},
                                df_f : {},
                                df_t : {},
                                t_f  : {},
                                t_df : {}
                            }
                        };

                        app.schema_browser_data.fields = response.fields;
                        app.schema_browser_data.key = response.info.key;

                        $.ajax
                        (
                            {
                                url : core_basepath + '/admin/luke?show=schema&wt=json',
                                dataType : 'json',
                                beforeSend : function( xhr, settings )
                                {
                                },
                                success : function( response, text_status, xhr )
                                {
                                    app.schema_browser_data.default_search_field = response.schema.defaultSearchField;
                                    app.schema_browser_data.unique_key_field = response.schema.uniqueKeyField;

                                    app.schema_browser_data.dynamic_fields = response.schema.dynamicFields;
                                    app.schema_browser_data.types = response.schema.types;

                                    var luke_array_to_struct = function( array )
                                    {
                                        var struct = {
                                            keys : [],
                                            values : []
                                        };
                                        for( var i = 0; i < array.length; i += 2 )
                                        {
                                            struct.keys.push( array[i] );
                                            struct.values.push( array[i+1] );
                                        }
                                        return struct;
                                    }

                                    var luke_array_to_hash = function( array )
                                    {
                                        var hash = {};
                                        for( var i = 0; i < array.length; i += 2 )
                                        {
                                            hash[ array[i] ] = array[i+1];
                                        }
                                        return hash;
                                    }

                                    for( var field in response.schema.fields )
                                    {
                                        app.schema_browser_data.fields[field] = $.extend
                                        (
                                            {},
                                            app.schema_browser_data.fields[field],
                                            response.schema.fields[field]
                                        );
                                    }

                                    for( var field in app.schema_browser_data.fields )
                                    {
                                        app.schema_browser_data.fields[field].copySourcesRaw = null;

                                        if( app.schema_browser_data.fields[field].copySources &&
                                            0 !== app.schema_browser_data.fields[field].copySources.length )
                                        {
                                            app.schema_browser_data.fields[field].copySourcesRaw =
                                                app.schema_browser_data.fields[field].copySources;
                                        }
                                        
                                        app.schema_browser_data.fields[field].copyDests = [];
                                        app.schema_browser_data.fields[field].copySources = [];
                                    }

                                    for( var field in app.schema_browser_data.fields )
                                    {
                                        if( app.schema_browser_data.fields[field].copySourcesRaw )
                                        {
                                            var copy_sources = app.schema_browser_data.fields[field].copySourcesRaw;
                                            for( var i in copy_sources )
                                            {
                                                var target = copy_sources[i].replace( /^.+:(.+)\{.+$/, '$1' );

                                                app.schema_browser_data.fields[field].copySources.push( target );
                                                app.schema_browser_data.fields[target].copyDests.push( field );
                                            }
                                        }

                                        app.schema_browser_data.relations.f_t[field] = app.schema_browser_data.fields[field].type;

                                        if( !app.schema_browser_data.relations.t_f[app.schema_browser_data.fields[field].type] )
                                        {
                                            app.schema_browser_data.relations.t_f[app.schema_browser_data.fields[field].type] = [];
                                        }
                                        app.schema_browser_data.relations.t_f[app.schema_browser_data.fields[field].type].push( field );

                                        if( app.schema_browser_data.fields[field].dynamicBase )
                                        {
                                            app.schema_browser_data.relations.f_df[field] = app.schema_browser_data.fields[field].dynamicBase;

                                            if( !app.schema_browser_data.relations.df_f[app.schema_browser_data.fields[field].dynamicBase] )
                                            {
                                                app.schema_browser_data.relations.df_f[app.schema_browser_data.fields[field].dynamicBase] = [];
                                            }
                                            app.schema_browser_data.relations.df_f[app.schema_browser_data.fields[field].dynamicBase].push( field );
                                        }
                                    }

                                    for( var dynamic_field in app.schema_browser_data.dynamic_fields )
                                    {
                                        app.schema_browser_data.relations.df_t[dynamic_field] = app.schema_browser_data.dynamic_fields[dynamic_field].type;

                                        if( !app.schema_browser_data.relations.t_df[app.schema_browser_data.dynamic_fields[dynamic_field].type] )
                                        {
                                            app.schema_browser_data.relations.t_df[app.schema_browser_data.dynamic_fields[dynamic_field].type] = [];
                                        }
                                        app.schema_browser_data.relations.t_df[app.schema_browser_data.dynamic_fields[dynamic_field].type].push( dynamic_field );
                                    }

                                    $.get
                                    (
                                        'tpl/schema-browser.html',
                                        function( template )
                                        {
                                            content_element
                                                .html( template );
                                            
                                            var schema_browser_element = $( '#schema-browser', content_element );
                                            var related_element = $( '#related', schema_browser_element );
                                            var related_select_element = $( 'select', related_element );
                                            var data_element = $( '#data', schema_browser_element );

                                            var related_options = '';
                                            
                                            var fields = [];
                                            for( var field_name in app.schema_browser_data.fields )
                                            {
                                                fields.push
                                                (
                                                    '<option value="/field/' + field_name + '">' + field_name + '</option>'
                                                );
                                            }
                                            if( 0 !== fields.length )
                                            {
                                                fields.sort();
                                                related_options += '<optgroup label="Fields">' + "\n";
                                                related_options += fields.sort().join( "\n" ) + "\n";
                                                related_options += '</optgroup>' + "\n";
                                            }
                                            
                                            var dynamic_fields = [];
                                            for( var type_name in app.schema_browser_data.dynamic_fields )
                                            {
                                                dynamic_fields.push
                                                (
                                                    '<option value="/dynamic-field/' + type_name + '">' + type_name + '</option>'
                                                );
                                            }
                                            if( 0 !== dynamic_fields.length )
                                            {
                                                dynamic_fields.sort();
                                                related_options += '<optgroup label="DynamicFields">' + "\n";
                                                related_options += dynamic_fields.sort().join( "\n" ) + "\n";
                                                related_options += '</optgroup>' + "\n";
                                            }
                                            
                                            var types = [];
                                            for( var type_name in app.schema_browser_data.types )
                                            {
                                                types.push
                                                (
                                                    '<option value="/type/' + type_name + '">' + type_name + '</option>'
                                                );
                                            }
                                            if( 0 !== types.length )
                                            {
                                                types.sort();
                                                related_options += '<optgroup label="Types">' + "\n";
                                                related_options += types.sort().join( "\n" ) + "\n";
                                                related_options += '</optgroup>' + "\n";
                                            }

                                            related_select_element
                                                .attr( 'rel', '#/' + $( 'p a', params.active_core ).html() + '/schema-browser' )
                                                .append( related_options );
                                            
                                            related_select_element
                                                .die( 'change' )
                                                .live
                                                (
                                                    'change',
                                                    function( event )
                                                    {
                                                        var select_element = $( this );
                                                        var option_element = $( 'option:selected', select_element );

                                                        location.href = select_element.attr( 'rel' ) + option_element.val();
                                                        return false;
                                                    }
                                                );

                                            params.schema_browser_element = schema_browser_element;
                                            sammy.trigger
                                            (
                                                'schema_browser_navi',
                                                params
                                            );
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
    }
);

// #/:core/schema-browser
sammy.get
(
    /^#\/([\w\d-]+)\/(schema-browser)$/,
    function( context )
    {
        var callback = function( schema_browser_data, data_element )
        {
            data_element
                .hide();
        };

        delete app.schema_browser_data;

        sammy.trigger
        (
            'schema_browser_load',
            {
                callback : callback,
                active_core : this.active_core
            }
        );
    }
);

// #/:core/schema-browser/field|dynamic-field|type/$field
sammy.get
(
    /^#\/([\w\d-]+)\/(schema-browser)(\/(field|dynamic-field|type)\/(.+))$/,
    function( context )
    {
        var core_basepath = this.active_core.attr( 'data-basepath' );

        var callback = function( schema_browser_data, data_element )
        {
            var field = context.params.splat[4];

            var type = context.params.splat[3];
            var is_f = 'field' === type;
            var is_df = 'dynamic-field' === type;
            var is_t = 'type' === type;
            
            var options_element = $( '.options', data_element );
            var sammy_basepath = context.path.indexOf( '/', context.path.indexOf( '/', 2 ) + 1 );

            data_element
                .show();

            var keystring_to_list = function( keystring, element_class )
            {
                var key_list = keystring.replace( /-/g, '' ).split( '' );
                var list = [];

                for( var i in key_list )
                {
                    var option_key = schema_browser_data.key[key_list[i]];

                    if( !option_key )
                    {
                        option_key = schema_browser_data.key[key_list[i].toLowerCase()];
                    }

                    if( !option_key )
                    {
                        option_key = schema_browser_data.key[key_list[i].toUpperCase()];
                    }

                    if( option_key )
                    {
                        list.push
                        (
                            '<dd ' + ( element_class ? ' class="' + element_class + '"' : '' ) + '>' +
                            option_key +
                            ',</dd>'
                        );
                    }
                }

                list[list.length-1] = list[key_list.length-1].replace( /,/, '' );

                return list;
            }

            var flags = null;

            if( is_f && schema_browser_data.fields[field] && schema_browser_data.fields[field].flags )
            {
                flags = schema_browser_data.fields[field].flags;
            }
            else if( is_df && schema_browser_data.dynamic_fields[field] && schema_browser_data.dynamic_fields[field].flags )
            {
                flags = schema_browser_data.dynamic_fields[field].flags;
            }

            // -- properties
            var properties_element = $( 'dt.properties', options_element );
            if( flags )
            {
                var properties_keys = keystring_to_list( flags, 'properties' );

                $( 'dd.properties', options_element )
                    .remove();

                properties_element
                    .show()
                    .after( properties_keys.join( "\n" ) );
            }
            else
            {
                $( '.properties', options_element )
                    .hide();
            }

            // -- schema
            var schema_element = $( 'dt.schema', options_element );
            if( is_f && schema_browser_data.fields[field] && schema_browser_data.fields[field].schema )
            {
                var schema_keys = keystring_to_list( schema_browser_data.fields[field].schema, 'schema' );

                $( 'dd.schema', options_element )
                    .remove();

                schema_element
                    .show()
                    .after( schema_keys.join( "\n" ) );
            }
            else
            {
                $( '.schema', options_element )
                    .hide();
            }

            // -- index
            var index_element = $( 'dt.index', options_element );
            if( is_f && schema_browser_data.fields[field] && schema_browser_data.fields[field].index )
            {
                var index_keys = [];

                if( 0 === schema_browser_data.fields[field].index.indexOf( '(' ) )
                {
                    index_keys.push( '<dd class="index">' + schema_browser_data.fields[field].index + '</dd>' );
                }
                else
                {
                    index_keys = keystring_to_list( schema_browser_data.fields[field].index, 'index' );
                }

                $( 'dd.index', options_element )
                    .remove();

                index_element
                    .show()
                    .after( index_keys.join( "\n" ) );
            }
            else
            {
                $( '.index', options_element )
                    .hide();
            }

            // -- docs
            var docs_element = $( 'dt.docs', options_element );
            if( is_f && schema_browser_data.fields[field] && schema_browser_data.fields[field].docs )
            {
                $( 'dd.docs', options_element )
                    .remove();

                docs_element
                    .show()
                    .after( '<dd class="docs">' + schema_browser_data.fields[field].docs + '</dd>' );
            }
            else
            {
                $( '.docs', options_element )
                    .hide();
            }

            // -- distinct 
            var distinct_element = $( 'dt.distinct', options_element );
            if( is_f && schema_browser_data.fields[field] && schema_browser_data.fields[field].distinct )
            {
                $( 'dd.distinct', options_element )
                    .remove();

                distinct_element
                    .show()
                    .after( '<dd class="distinct">' + schema_browser_data.fields[field].distinct + '</dd>' );
            }
            else
            {
                $( '.distinct', options_element )
                    .hide();
            }

            // -- position-increment-gap 
            var pig_element = $( 'dt.position-increment-gap', options_element );
            if( is_f && schema_browser_data.fields[field] && schema_browser_data.fields[field].positionIncrementGap )
            {
                $( 'dt.position-increment-gap', options_element )
                    .remove();

                pig_element
                    .show()
                    .after( '<dd class="position-increment-gap">' + schema_browser_data.fields[field].positionIncrementGap + '</dd>' );
            }
            else
            {
                $( '.position-increment-gap', options_element )
                    .hide();
            }
            
            var analyzer_element = $( '.analyzer', data_element );
            var analyzer_data = null;

            if( is_f )
            {
                analyzer_data = schema_browser_data.types[schema_browser_data.relations.f_t[field]];
            }
            else if( is_df )
            {
                analyzer_data = schema_browser_data.types[schema_browser_data.relations.df_t[field]];
            }
            else if( is_t )
            {
                analyzer_data = schema_browser_data.types[field];
            }

            if( analyzer_data )
            {
                var transform_analyzer_data_into_list = function( analyzer_data )
                {
                    var args = [];
                    for( var key in analyzer_data.args )
                    {
                        var arg_class = '';
                        var arg_content = '';

                        if( 'true' === analyzer_data.args[key] || '1' === analyzer_data.args[key] )
                        {
                            arg_class = 'ico-1';
                            arg_content = key;
                        }
                        else if( 'false' === analyzer_data.args[key] || '0' === analyzer_data.args[key] )
                        {
                            arg_class = 'ico-0';
                            arg_content = key;
                        }
                        else
                        {
                            arg_content = key + ': ';

                            if( 'synonyms' === key || 'words' === key )
                            {
                                // @TODO: set link target for file
                                arg_content += '<a>' + analyzer_data.args[key] + '</a>';
                            }
                            else
                            {
                                arg_content += analyzer_data.args[key];
                            }
                        }

                        args.push( '<dd class="' + arg_class + '">' + arg_content + '</dd>' );
                    }

                    var list_content = '<dt>' + analyzer_data.className + '</dt>';
                    if( 0 !== args.length )
                    {
                        args.sort();
                        list_content += args.join( "\n" );
                    }

                    return list_content;
                }

                // -- field-type
                var field_type_element = $( 'dt.field-type', options_element );

                $( 'dd.field-type', options_element )
                    .remove();

                field_type_element
                    .show()
                    .after( '<dd class="field-type">' + analyzer_data.className + '</dd>' );


                for( var key in analyzer_data )
                {
                    var key_match = key.match( /^(.+)Analyzer$/ );
                    if( !key_match )
                    {
                        continue;
                    }

                    var analyzer_key_element = $( '.' + key_match[1], analyzer_element );
                    var analyzer_key_data = analyzer_data[key];

                    analyzer_element.show();
                    analyzer_key_element.show();

                    if( analyzer_key_data.className )
                    {
                        $( 'dl:first dt', analyzer_key_element )
                            .html( analyzer_key_data.className );
                    }

                    $( 'ul li', analyzer_key_element )
                        .hide();

                    for( var type in analyzer_key_data )
                    {
                        if( 'object' !== typeof analyzer_key_data[type] )
                        {
                            continue;
                        }

                        var type_element = $( '.' + type, analyzer_key_element );
                        var type_content = [];

                        type_element.show();

                        if( analyzer_key_data[type].className )
                        {
                            type_content.push( transform_analyzer_data_into_list( analyzer_key_data[type] ) );
                        }
                        else
                        {
                            for( var entry in analyzer_key_data[type] )
                            {
                                type_content.push( transform_analyzer_data_into_list( analyzer_key_data[type][entry] ) );
                            }
                        }

                        $( 'dl', type_element )
                            .empty()
                            .append( type_content.join( "\n" ) );
                    }
                }
            }

            var terminfo_element = $( '.terminfo-holder', data_element );

            if( !is_f )
            {
                terminfo_element
                    .hide();
            }
            else
            {
                terminfo_element
                    .show();

                var status_element = $( '.status', terminfo_element );
                
                $.ajax
                (
                    {
                        url : core_basepath + '/admin/luke?numTerms=50&wt=json&fl=' + field,
                        dataType : 'json',
                        context : terminfo_element,
                        beforeSend : function( xhr, settings )
                        {
                        },
                        success : function( response, text_status, xhr )
                        {
                            status_element
                                .hide();

                            var field_data = response.fields[field];

                            var topterms_holder_element = $( '.topterms-holder', data_element );
                            var histogram_holder_element = $( '.histogram-holder', data_element );

                            var luke_array_to_struct = function( array )
                            {
                                var struct = {
                                    keys : [],
                                    values : []
                                };
                                for( var i = 0; i < array.length; i += 2 )
                                {
                                    struct.keys.push( array[i] );
                                    struct.values.push( array[i+1] );
                                }
                                return struct;
                            }

                            var luke_array_to_hash = function( array )
                            {
                                var hash = {};
                                for( var i = 0; i < array.length; i += 2 )
                                {
                                    hash[ array[i] ] = array[i+1];
                                }
                                return hash;
                            }

                            if( !field_data.topTerms )
                            {
                                topterms_holder_element
                                    .hide();
                            }
                            else
                            {
                                topterms_holder_element
                                    .show();

                                var topterms_table_element = $( 'table', topterms_holder_element );

                                var topterms_navi_less = $( 'p.navi .less', topterms_holder_element );
                                var topterms_navi_more = $( 'p.navi .more', topterms_holder_element );

                                var topterms_count = luke_array_to_struct( field_data.topTerms ).keys.length; 
                                var topterms_hash = luke_array_to_hash( field_data.topTerms );
                                var topterms_content = '<tbody>';

                                var i = 1;
                                for( var term in topterms_hash )
                                {
                                    topterms_content += '<tr>' + "\n" +
                                                        '<td class="position">' + i + '</td>' + "\n" + 
                                                        '<td class="term">' + term + '</td>' + "\n" + 
                                                        '<td class="frequency">' + topterms_hash[term] + '</td>' + "\n" + 
                                                        '</tr>' + "\n";

                                    if( i !== topterms_count && 0 === i % 10 )
                                    {
                                        topterms_content += '</tbody><tbody>';
                                    }

                                    i++;
                                }

                                topterms_content += '</tbody>';

                                topterms_table_element
                                    .empty()
                                    .append( topterms_content );
                                
                                $( 'tbody', topterms_table_element )
                                    .die( 'change' )
                                    .live
                                    (
                                        'change',
                                        function()
                                        {
                                            var blocks = $( 'tbody', topterms_table_element );
                                            var visible_blocks = blocks.filter( ':visible' );
                                            var hidden_blocks = blocks.filter( ':hidden' );

                                            $( 'p.head .shown', topterms_holder_element )
                                                .html( $( 'tr', visible_blocks ).size() );

                                            0 < hidden_blocks.size()
                                                ? topterms_navi_more.show()
                                                : topterms_navi_more.hide();

                                            1 < visible_blocks.size()
                                                ? topterms_navi_less.show()
                                                : topterms_navi_less.hide();
                                        }
                                    );

                                $( 'tbody tr:odd', topterms_table_element )
                                    .addClass( 'odd' );

                                $( 'tbody:first', topterms_table_element )
                                    .show()
                                    .trigger( 'change' );

                                $( 'p.head .max', topterms_holder_element )
                                    .html( field_data.distinct );

                                topterms_navi_less
                                    .die( 'click' )
                                    .live
                                    (
                                        'click',
                                        function( event )
                                        {
                                            $( 'tbody:visible', topterms_table_element ).last()
                                                .hide()
                                                .trigger( 'change' );
                                        }
                                    );

                                topterms_navi_more
                                    .die( 'click' )
                                    .live
                                    (
                                        'click',
                                        function( event )
                                        {
                                            $( 'tbody:hidden', topterms_table_element ).first()
                                                .show()
                                                .trigger( 'change' );
                                        }
                                    );
                            }

                            if( !field_data.histogram )
                            {
                                histogram_holder_element
                                    .hide();
                            }
                            else
                            {
                                histogram_holder_element
                                    .show();

                                var histogram_element = $( '.histogram', histogram_holder_element );

                                var histogram_values = luke_array_to_hash( field_data.histogram );
                                var histogram_legend = '';

                                histogram_holder_element
                                    .show();

                                for( var key in histogram_values )
                                {
                                    histogram_legend += '<dt><span>' + key + '</span></dt>' + "\n" +
                                                        '<dd title="' + key + '">' +
                                                        '<span>' + histogram_values[key] + '</span>' +
                                                        '</dd>' + "\n";
                                }

                                $( 'dl', histogram_holder_element )
                                    .html( histogram_legend );

                                histogram_element
                                    .sparkline
                                    (
                                        luke_array_to_struct( field_data.histogram ).values,
                                        {
                                            type : 'bar',
                                            barColor : '#c0c0c0',
                                            zeroColor : '#ffffff',
                                            height : histogram_element.height(),
                                            barWidth : 46,
                                            barSpacing : 3
                                        }
                                    );
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

            }
        }

        sammy.trigger
        (
            'schema_browser_load',
            {
                callback : callback,
                active_core : this.active_core,
                route_params : this.params
            }
        );
    }
);
require 'date' unless RUBY_PLATFORM == 'opal'

# Add custom functions to this module that you want to use in your Slim
# templates. Within the template you can invoke them as top-level functions
# just like in Haml.
module Slim::Helpers

  # URIs of external assets.
  FONT_AWESOME_URI     = '//cdn.jsdelivr.net/npm/font-awesome@4.7.0/css/font-awesome.min.css'
  HIGHLIGHTJS_BASE_URI = '//cdn.jsdelivr.net/gh/highlightjs/cdn-release@9.12.0/build/'
  KATEX_CSS_URI        = '//cdn.jsdelivr.net/npm/katex@0.8.3/dist/katex.min.css'
  KATEX_JS_URI         = '//cdn.jsdelivr.net/npm/katex@0.8.3/dist/katex.min.js'

  # Defaults
  DEFAULT_HIGHLIGHTJS_THEME = 'github'
  DEFAULT_SECTNUMLEVELS = 3
  DEFAULT_TOCLEVELS = 2

  KATEX_RENDER_CODE = <<-JS.gsub(/\s+/, ' ')
    document.addEventListener("DOMContentLoaded", function() {
      var elements = document.getElementsByClassName("math");
      for (var i = 0; i < elements.length; i++) {
        var el = elements[i];
        if (el.getAttribute("data-lang") !== "tex") {
          continue;
        }
        katex.render(el.textContent.slice(2, -2), el, {
          "displayMode": el.nodeName.toUpperCase() !== "SPAN",
          "throwOnError": false,
        });
      }
    });
  JS

  VOID_ELEMENTS = %w(area base br col command embed hr img input keygen link
                     meta param source track wbr)


  # @return [Logger]
  def log
    @_html5s_logger ||= ::Asciidoctor::Html5s::Logging.default_logger
  end

  ##
  # Captures the given block for later yield.
  #
  # @example Basic capture usage.
  #   - capture
  #     img src=image_uri
  #   - if title?
  #     figure.image
  #       - yield_capture
  #       figcaption =captioned_title
  #   - else
  #     - yield_capture
  #
  # @example Capture with passing parameters.
  #   - capture do |id|
  #     img src=image_uri
  #   - if title?
  #     figure id=@id
  #       - yield_capture
  #       figcaption =caption
  #   - else
  #     - yield_capture @id
  #
  # @see yield_capture
  def capture(&block)
    @_html5s_capture = block
    nil
  end

  ##
  # Yields the captured block (see {#capture}).
  #
  # @param *params parameters to pass to the block.
  # @return A content of the captured block.
  # @see capture
  def yield_capture(*params)
    @_html5s_capture.call(*params) if @_html5s_capture
  end

  ##
  # Creates an HTML tag with the given name and optionally attributes. Can take
  # a block that will run between the opening and closing tags.
  #
  # @param name [#to_s] the name of the tag.
  # @param attributes [Hash] (default: {})
  # @param content [#to_s] the content; +nil+ to call the block. (default: nil).
  # @yield The block of Slim/HTML code within the tag (optional).
  # @return [String] a rendered HTML element.
  #
  def html_tag(name, attributes = {}, content = nil)
    attrs = attributes.inject([]) do |attrs, (k, v)|
      next attrs if !v || v.nil_or_empty?
      v = v.compact.join(' ') if v.is_a? Array
      attrs << (v == true ? k : %(#{k}="#{v}"))
    end
    attrs_str = attrs.empty? ? '' : ' ' + attrs.join(' ')

    if VOID_ELEMENTS.include? name.to_s
      %(<#{name}#{attrs_str}>)
    else
      content ||= yield if block_given?
      %(<#{name}#{attrs_str}>#{content}</#{name}>)
    end
  end

  ##
  # Conditionally wraps a block in an element. If condition is +true+ then it
  # renders the specified tag with optional attributes and the given
  # block inside, otherwise it just renders the block.
  #
  # For example:
  #
  #    = html_tag_if link?, 'a', {class: 'image', href: (attr :link)}
  #      img src='./img/tux.png'
  #
  # will produce:
  #
  #    <a href="http://example.org" class="image">
  #      <img src="./img/tux.png">
  #    </a>
  #
  # if +link?+ is truthy, and just
  #
  #   <img src="./img/tux.png">
  #
  # otherwise.
  #
  # @param condition [Boolean] the condition to test to determine whether to
  #        render the enclosing tag.
  # @param name (see #html_tag)
  # @param attributes (see #html_tag)
  # @param content (see #html_tag)
  # @yield (see #html_tag)
  # @return [String] a rendered HTML fragment.
  #
  def html_tag_if(condition, name, attributes = {}, content = nil, &block)
    if condition
      html_tag name, attributes, content, &block
    else
      content || yield
    end
  end

  ##
  # Wraps a block in a div element with the specified class and optionally
  # the node's +id+ and +role+(s). If the node's +title+ is not empty, then a
  # nested div with the class "title" and the title's content is added as well.
  #
  # @example When @id, @role and @title attributes are set.
  #   = block_with_title :class=>['quote-block', 'center']
  #     blockquote =content
  #
  #   <section id="myid" class="quote-block center myrole1 myrole2">
  #     <h6>Block Title</h6>
  #     <blockquote>Lorem ipsum</blockquote>
  #   </section>
  #
  # @example When @id, @role and @title attributes are empty.
  #   = block_with_title :class=>'quote-block center', :style=>style_value(float: 'left')
  #     blockquote =content
  #
  #   <div class="quote-block center" style="float: left;">
  #     <blockquote>Lorem ipsum</blockquote>
  #   </div>
  #
  # @example When shorthand style for class attribute is used.
  #   = block_with_title 'quote-block center'
  #     blockquote =content
  #
  #   <div class="quote-block center">
  #     <blockquote>Lorem ipsum</blockquote>
  #   </div>
  #
  # @param attrs [Hash, String] the tag's attributes as Hash),
  #        or the tag's class if it's not a Hash.
  # @param title [String, nil] the title.
  # @yield The block of Slim/HTML code within the tag (optional).
  # @return [String] a rendered HTML fragment.
  #
  def block_with_title(attrs = {}, title = @title, &block)
    if (klass = attrs[:class]).is_a? String
      klass = klass.split(' ')
    end
    attrs[:class] = [klass, role].flatten.uniq
    attrs[:id] = id

    if title.nil_or_empty?
      # XXX quick hack
      nested = is_a?(::Asciidoctor::List) &&
          (parent.is_a?(::Asciidoctor::ListItem) || parent.is_a?(::Asciidoctor::List))
      html_tag_if !nested, :div, attrs, yield
    else
      html_tag :section, attrs do
        [html_tag(:h6, {class: 'block-title'}, title), yield].join("\n")
      end
    end
  end

  def block_with_caption(position = :bottom, attrs = {}, &block)
    if (klass = attrs[:class]).is_a? String
      klass = klass.split(' ')
    end
    attrs[:class] = [klass, role].flatten.uniq
    attrs[:id] = id

    if title.nil_or_empty?
      html_tag :div, attrs, yield
    else
      html_tag :figure, attrs do
        ary = [yield, html_tag(:figcaption) { captioned_title }]
        ary.reverse! if position == :top
        ary.compact.join("\n")
      end
    end
  end

  ##
  # Delimite the given equation as a STEM of the specified type.
  #
  # Note: This is not needed nor used for KaTeX, but keep this for the case
  # user wants to use a different method.
  #
  # @param equation [String] the equation to delimite.
  # @param type [#to_sym] the type of the STEM renderer (latexmath, or asciimath).
  # @return [String] the delimited equation.
  #
  def delimit_stem(equation, type)
    if (@_html5s_stem_type ||= document.attr('html5s-force-stem-type'))
      type = @_html5s_stem_type
    end

    if is_a? ::Asciidoctor::Block
      open, close = ::Asciidoctor::BLOCK_MATH_DELIMITERS[type.to_sym]
    else
      open, close = ::Asciidoctor::INLINE_MATH_DELIMITERS[type.to_sym]
    end

    if !equation.start_with?(open) || !equation.end_with?(close)
      equation = [open, equation, close].join
    end
    equation
  end

  ##
  # Formats the given hash as CSS declarations for an inline style.
  #
  # @example
  #   style_value(text_align: 'right', float: 'left')
  #   => "text-align: right; float: left;"
  #
  #   style_value(text_align: nil, float: 'left')
  #   => "float: left;"
  #
  #   style_value(width: [90, '%'], height: '50px')
  #   => "width: 90%; height: 50px;"
  #
  #   style_value(width: ['120px', 'px'])
  #   => "width: 90px;"
  #
  #   style_value(width: [nil, 'px'])
  #   => nil
  #
  # @param declarations [Hash]
  # @return [String, nil]
  #
  def style_value(declarations)
    decls = []

    declarations.each do |prop, value|
      next if value.nil?

      if value.is_a? Array
        value, unit = value
        next if value.nil?
        value = value.to_s + unit unless value.end_with? unit
      end
      prop = prop.to_s.gsub('_', '-')
      decls << "#{prop}: #{value}"
    end

    decls.empty? ? nil : decls.join('; ') + ';'
  end

  def urlize(*segments)
    path = segments * '/'
    if path.start_with? '//'
      @_html5s_uri_scheme ||= document.attr('asset-uri-scheme', 'https')
      path = "#{@_html5s_uri_scheme}:#{path}" unless @_html5s_uri_scheme.empty?
    end
    normalize_web_path path
  end


  ##
  # Gets the value of the specified attribute in this node.
  #
  # This is just an alias for +attr+ method with disabled _inherit_ to make it
  # more clear.
  #
  # @param name [String, Symbol] the name of the attribute to lookup.
  # @param default_val the value to return if the attribute is not found.
  # @return value of the attribute or +default_val+ if not found.
  #
  def local_attr(name, default_val = nil)
    attr(name, default_val, false)
  end

  ##
  # Checks if the attribute is defined on this node, optionally performing
  # a comparison of its value if +expect_val+ is not nil.
  #
  # This is just an alias for +attr?+ method with disabled _inherit_ to make it
  # more clear.
  #
  # @param name [String, Symbol] the name of the attribute to lookup.
  # @param default_val the expected value of the attribute.
  # @return [Boolean] whether the attribute exists and, if +expect_val+ is
  #   specified, whether the value of the attribute matches the +expect_val+.
  #
  def local_attr?(name, expect_val = nil)
    attr?(name, expect_val, false)
  end

  ##
  # @param index [Integer] the footnote's index.
  # @return [String] footnote id to be used in a link.
  def footnote_id(index = local_attr(:index))
    "_footnote_#{index}"
  end

  ##
  # @param index (see #footnote_id)
  # @return [String] footnoteref id to be used in a link.
  def footnoteref_id(index = local_attr(:index))
    "_footnoteref_#{index}"
  end

  def nowrap?
    'nowrap' if !document.attr?(:prewrap) || option?('nowrap')
  end

  def print_item_content(item)
    wrap = item.blocks? && !item.blocks.all? { |b| b.is_a? ::Asciidoctor::List }
    [ (html_tag_if(wrap, :p) { item.text } if item.text?), item.content ].join
  end

  ##
  # Returns corrected section level.
  #
  # @param sec [Asciidoctor::Section] the section node (default: self).
  # @return [Integer]
  #
  def section_level(sec = self)
    (sec.level == 0 && sec.special) ? 1 : sec.level
  end

  ##
  # Returns the captioned section's title, optionally numbered.
  #
  # @param sec [Asciidoctor::Section] the section node (default: self).
  # @return [String]
  #
  def section_title(sec = self)
    sectnumlevels = document.attr(:sectnumlevels, DEFAULT_SECTNUMLEVELS).to_i

    if sec.numbered && !sec.caption && sec.level <= sectnumlevels
      [sec.sectnum, sec.captioned_title].join(' ')
    else
      sec.captioned_title
    end
  end

  ##
  # @return [String] language of STEM block or inline node (tex or asciimath).
  def stem_lang
    value = (inline? ? type : style).to_s
    value == 'latexmath' ? 'tex' : value
  end

  def link_rel
    'noopener' if option?('noopener') || attr(:window) == '_blank'
  end

  #--------------------------------------------------------
  # block_admonition
  #

  ##
  # @return [Boolean] should be this admonition wrapped in aside element?
  def admonition_aside?
    %w[note tip].include? attr(:name)
  end

  ##
  # @return [String, nil] WAI-ARIA role of this admonition.
  def admonition_aria
    case attr(:name)
    when 'note'
      'note'  # https://www.w3.org/TR/wai-aria/roles#note
    when 'tip'
      'doc-tip'  # https://www.w3.org/TR/dpub-aria-1.0/#doc-tip
    when 'caution', 'important', 'warning'
      'doc-notice'  # https://www.w3.org/TR/dpub-aria-1.0/#doc-notice
    end
  end

  #--------------------------------------------------------
  # block_listing
  #

  ##
  # @return [String] a canonical name of the source-highlighter to be used as
  #         a style class.
  def highlighter
    @_html5s_highlighter ||=
      case (highlighter = document.attr('source-highlighter'))
      when 'coderay'; 'CodeRay'
      when 'highlight.js'; 'highlightjs'
      else highlighter
      end
  end

  ##
  # Returns the callout list attached to this listing node, or +nil+ if none.
  #
  # Note: This variable is set by extension
  # {Asciidoctor::Html5s::AttachedColistTreeprocessor}.
  #
  # @return [Asciidoctor::List, nil]
  def callout_list
    @html5s_colist
  end

  def source_lang
    local_attr :language, false
  end

  #--------------------------------------------------------
  # block_open
  #

  ##
  # Returns +true+ if an abstract block is allowed in this document type,
  # otherwise prints warning and returns +false+.
  def abstract_allowed?
    if result = (parent == document && document.doctype == 'book')
      log.warn 'asciidoctor: WARNING: abstract block cannot be used in a document
without a title when doctype is book. Excluding block content.'
    end
    !result
  end

  ##
  # Returns +true+ if a partintro block is allowed in this context, otherwise
  # prints warning and returns +false+.
  def partintro_allowed?
    if result = (level != 0 || parent.context != :section || document.doctype != 'book')
      log.warn "asciidoctor: ERROR: partintro block can only be used when doctype
is book and it's a child of a book part. Excluding block content."
    end
    !result
  end

  #--------------------------------------------------------
  # block_table
  #

  def autowidth?
    option? :autowidth
  end

  def spread?
    if !autowidth? || local_attr?('width')
      'spread' if attr? :tablepcwidth, 100
    end
  end

  #--------------------------------------------------------
  # block_video
  #

  # @return [Boolean] +true+ if the video should be embedded in an iframe.
  def video_iframe?
    ['vimeo', 'youtube'].include? attr(:poster)
  end

  def video_uri
    case attr(:poster, '').to_sym
    when :vimeo
      params = {
        autoplay: (1 if option? 'autoplay'),
        loop:     (1 if option? 'loop')
      }
      start_anchor = "#at=#{attr :start}" if attr? :start
      "//player.vimeo.com/video/#{attr :target}#{start_anchor}#{url_query params}"

    when :youtube
      video_id, list_id = attr(:target).split('/', 2)
      params = {
        rel:      0,
        start:    (attr :start),
        end:      (attr :end),
        list:     (attr :list, list_id),
        autoplay: (1 if option? 'autoplay'),
        loop:     (1 if option? 'loop'),
        controls: (0 if option? 'nocontrols')
      }
      "//www.youtube.com/embed/#{video_id}#{url_query params}"
    else
      anchor = [attr(:start), attr(:end)].join(',').chomp(',')
      anchor = '#t=' + anchor unless anchor.empty?
      media_uri "#{attr :target}#{anchor}"
    end
  end

  # Formats URL query parameters.
  def url_query(params)
    str = params.map { |k, v|
      next if v.nil? || v.to_s.empty?
      [k, v] * '='
    }.compact.join('&amp;')

    '?' + str unless str.empty?
  end

  #--------------------------------------------------------
  # document
  #

  ##
  # @return [String, nil] the revision date in ISO 8601, or nil if not
  #   available or in invalid format.
  def revdate_iso
    ::Date.parse(revdate).iso8601 if defined? ::Date
  rescue ArgumentError
    nil
  end

  ##
  # Returns HTML meta tag if the given +content+ is not +nil+.
  #
  # @param name [#to_s] the name for the metadata.
  # @param content [#to_s, nil] the value of the metadata, or +nil+.
  # @return [String, nil] the meta tag, or +nil+ if the +content+ is +nil+.
  #
  def html_meta_if(name, content)
    %(<meta name="#{name}" content="#{content}">) if content
  end

  # Returns formatted style/link and script tags for header.
  def styles_and_scripts
    scripts = []
    styles = []
    tags = []

    stylesheet = attr :stylesheet
    stylesdir = attr :stylesdir, ''
    default_style = ::Asciidoctor::DEFAULT_STYLESHEET_KEYS.include? stylesheet
    linkcss = attr?(:linkcss) || safe >= ::Asciidoctor::SafeMode::SECURE
    ss = ::Asciidoctor::Stylesheets.instance

    if linkcss
      path = default_style ? ::Asciidoctor::DEFAULT_STYLESHEET_NAME : stylesheet
      styles << { href: [stylesdir, path] }
    elsif default_style
      styles << { text: ss.primary_stylesheet_data }
    else
      styles << { text: read_asset(normalize_system_path(stylesheet, stylesdir), true) }
    end

    if attr? :icons, 'font'
      if attr? 'iconfont-remote'
        styles << { href: attr('iconfont-cdn', FONT_AWESOME_URI) }
      else
        styles << { href: [stylesdir, "#{attr 'iconfont-name', 'font-awesome'}.css"] }
      end
    end

    if attr? 'stem'
      styles << { href: KATEX_CSS_URI }
      scripts << { src: KATEX_JS_URI }
      scripts << { text: KATEX_RENDER_CODE }
    end

    case attr 'source-highlighter'
    when 'coderay'
      if attr('coderay-css', 'class') == 'class'
        if linkcss
          styles << { href: [stylesdir, ss.coderay_stylesheet_name] }
        else
          styles << { text: ss.coderay_stylesheet_data }
        end
      end

    when 'highlightjs'
      hjs_base = attr :highlightjsdir, HIGHLIGHTJS_BASE_URI
      hjs_theme = attr 'highlightjs-theme', DEFAULT_HIGHLIGHTJS_THEME

      scripts << { src: [hjs_base, 'highlight.min.js'] }
      scripts << { text: 'hljs.initHighlightingOnLoad()' }
      styles  << { href: [hjs_base, "styles/#{hjs_theme}.min.css"] }
    end

    styles.each do |item|
      if item.key?(:text)
        tags << html_tag(:style) { item[:text] }
      else
        tags << html_tag(:link, rel: 'stylesheet', href: urlize(*item[:href]))
      end
    end

    scripts.each do |item|
      if item.key? :text
        tags << html_tag(:script, type: item[:type]) { item[:text] }
      else
        tags << html_tag(:script, type: item[:type], src: urlize(*item[:src]))
      end
    end

    tags.join("\n")
  end

  #--------------------------------------------------------
  # inline_anchor
  #

  # @return [String] text of the xref anchor.
  def xref_text
    str =
      if text
        text
      elsif (path = local_attr :path)
        path
      elsif document.respond_to? :catalog  # Asciidoctor >=1.5.6
        ref = document.catalog[:refs][attr :refid]
        if ref.kind_of? Asciidoctor::AbstractNode
          ref.xreftext((@_html5s_xrefstyle ||= document.attributes['xrefstyle']))
        end
      else  # Asciidoctor < 1.5.6
        document.references[:ids][attr :refid || target]
      end
    (str || "[#{attr :refid}]").tr_s("\n", ' ')
  end

  # @return [String, nil] text of the bibref anchor, or +nil+ if not found.
  def bibref_text
    if document.respond_to? :catalog  # Asciidoctor >=1.5.6
      # NOTE: Technically it should be `reftext`, but subs have already been applied to text.
      text
    else
      "[#{target}]"
    end
  end

  #--------------------------------------------------------
  # inline_image
  #

  # @return [Array] style classes for a Font Awesome icon.
  def icon_fa_classes
    [ "fa fa-#{target}",
      ("fa-#{attr :size}" if attr? :size),
      ("fa-rotate-#{attr :rotate}" if attr? :rotate),
      ("fa-flip-#{attr :flip}" if attr? :flip)
    ].compact
  end
end

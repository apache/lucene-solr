# Provides a tag for embedding sparklines graphs into your Rails app.
#
module SparklinesHelper

	# Call with an array of data and a hash of params for the Sparklines module.
  #
  #  sparkline_tag [42, 37, 43, 182], :type => 'bar', :line_color => 'black'
  #
	# You can also pass :class => 'some_css_class' ('sparkline' by default).
	def sparkline_tag(results=[], options={})		
		url = { :controller => 'sparklines',
			:results => results.join(',') }
		options = url.merge(options)
		
		%(<img src="#{ url_for options }" class="#{options[:class] || 'sparkline'}" alt="Sparkline Graph" />)
	end

end

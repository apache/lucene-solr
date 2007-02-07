
require 'rubygems'
require 'RMagick'

=begin rdoc

A library for generating small unmarked graphs (sparklines).

Can be used to write an image to a file or make a web service with Rails or other Ruby CGI apps.

Idea and much of the outline for the source lifted directly from {Joe Gregorio's Python Sparklines web service script}[http://bitworking.org/projects/sparklines].

Requires the RMagick image library.

==Authors

{Dan Nugent}[mailto:nugend@gmail.com] Original port from Python Sparklines library.

{Geoffrey Grosenbach}[mailto:boss@topfunky.com] -- http://nubyonrails.topfunky.com 
-- Conversion to module and further maintenance.

==General Usage and Defaults

To use in a script:

	require 'rubygems'
	require 'sparklines'
	Sparklines.plot([1,25,33,46,89,90,85,77,42], 
	                :type => 'discrete', 
	                :height => 20)

An image blob will be returned which you can print, write to STDOUT, etc.

For use with Ruby on Rails, see the sparklines plugin:

  http://nubyonrails.com/pages/sparklines

In your view, call it like this:

  <%= sparkline_tag [1,2,3,4,5,6] %>

Or specify details:

  <%= sparkline_tag [1,2,3,4,5,6], 
                    :type => 'discrete', 
                    :height => 10, 
                    :upper => 80, 
                    :above_color => 'green', 
                    :below_color => 'blue' %>

Graph types:

 area
 discrete
 pie
 smooth
 bar
 whisker

General Defaults:

 :type              => 'smooth'
 :height            => 14px
 :upper             => 50
 :above_color       => 'red'
 :below_color       => 'grey'
 :background_color  => 'white'
 :line_color        => 'lightgrey'

==License

Licensed under the MIT license.

=end
class Sparklines

  VERSION = '0.4.1'

  @@label_margin = 5.0
  @@pointsize = 10.0

  class << self

    # Does the actual plotting of the graph. 
    # Calls the appropriate subclass based on the :type argument. 
    # Defaults to 'smooth'
    def plot(data=[], options={})
      defaults = {
        :type => 'smooth',
        :height => 14,
        :upper => 50,
        :diameter => 20,
        :step => 2,
        :line_color => 'lightgrey',

        :above_color => 'red',
        :below_color => 'grey',
        :background_color => 'white',
        :share_color => 'red',
        :remain_color => 'lightgrey',
        :min_color => 'blue',
        :max_color => 'green',	
        :last_color => 'red',                

        :has_min => false,
        :has_max => false,
        :has_last => false,

        :label => nil
      }

      # HACK for HashWithIndifferentAccess
      options_sym = Hash.new
      options.keys.each do |key|
        options_sym[key.to_sym] = options[key]
      end

      options_sym  = defaults.merge(options_sym)
    	
      # Call the appropriate method for actual plotting.
      sparkline = self.new(data, options_sym)
      if %w(area bar pie smooth discrete whisker).include? options_sym[:type]
        sparkline.send options_sym[:type]
      else
        sparkline.plot_error options_sym
      end
    end

    # Writes a graph to disk with the specified filename, or "sparklines.png"
    def plot_to_file(filename="sparklines.png", data=[], options={})
      File.open( filename, 'wb' ) do |png|
        png << self.plot( data, options)
      end
    end

  end # class methods

  def initialize(data=[], options={})
    @data = Array(data)
    @options = options
    normalize_data
  end

  ##
  # Creates a continuous area sparkline. Relevant options.
  #
  # :step - An integer that determines the distance between each point on the sparkline.  Defaults to 2.
  #
  # :height - An integer that determines what the height of the sparkline will be.  Defaults to 14
  #
  # :upper - An integer that determines the threshold for colorization purposes.  Any value less than upper will be colored using below_color, anything above and equal to upper will use above_color.  Defaults to 50.
  #
  # :has_min - Determines whether a dot will be drawn at the lowest value or not.  Defaults to false.
  #
  # :has_max - Determines whether a dot will be drawn at the highest value or not.  Defaults to false.
  #
  # :has_last - Determines whether a dot will be drawn at the last value or not.  Defaults to false.
  #
  # :min_color - A string or color code representing the color that the dot drawn at the smallest value will be displayed as.  Defaults to blue.
  #
  # :max_color - A string or color code representing the color that the dot drawn at the largest value will be displayed as.  Defaults to green.
  #
  # :last_color - A string or color code representing the color that the dot drawn at the last value will be displayed as.  Defaults to red.
  #
  # :above_color - A string or color code representing the color to draw values above or equal the upper value.  Defaults to red.
  #
  # :below_color - A string or color code representing the color to draw values below the upper value. Defaults to gray.

  def area

    step = @options[:step].to_i
    height = @options[:height].to_i
    background_color = @options[:background_color]
    
    create_canvas((@norm_data.size - 1) * step + 4, height, background_color)

    upper = @options[:upper].to_i

    has_min = @options[:has_min]
    has_max = @options[:has_max]
    has_last = @options[:has_last]

    min_color = @options[:min_color]
    max_color = @options[:max_color]
    last_color = @options[:last_color]
    below_color = @options[:below_color]
    above_color = @options[:above_color]


    coords = [[0,(height - 3 - upper/(101.0/(height-4)))]]
    i=0
    @norm_data.each do |r|
        coords.push [(2 + i), (height - 3 - r/(101.0/(height-4)))]
        i += step
    end
    coords.push [(@norm_data.size - 1) * step + 4, (height - 3 - upper/(101.0/(height-4)))]

    # TODO Refactor! Should take a block and do both.
    #
    # Block off the bottom half of the image and draw the sparkline
    @draw.fill(above_color)
    @draw.define_clip_path('top') do
      @draw.rectangle(0,0,(@norm_data.size - 1) * step + 4,(height - 3 - upper/(101.0/(height-4))))
    end
    @draw.clip_path('top')
    @draw.polygon *coords.flatten

    # Block off the top half of the image and draw the sparkline
    @draw.fill(below_color)
    @draw.define_clip_path('bottom') do
      @draw.rectangle(0,(height - 3 - upper/(101.0/(height-4))),(@norm_data.size - 1) * step + 4,height)
    end
    @draw.clip_path('bottom')
    @draw.polygon *coords.flatten

    # The sparkline looks kinda nasty if either the above_color or below_color gets the center line
    @draw.fill('black')
    @draw.line(0,(height - 3 - upper/(101.0/(height-4))),(@norm_data.size - 1) * step + 4,(height - 3 - upper/(101.0/(height-4))))

    # After the parts have been masked, we need to let the whole canvas be drawable again
    # so a max dot can be displayed
    @draw.define_clip_path('all') do
      @draw.rectangle(0,0,@canvas.columns,@canvas.rows)
    end
    @draw.clip_path('all')
        
    drawbox(coords[@norm_data.index(@norm_data.min)+1], 1, min_color) if has_min == true
    drawbox(coords[@norm_data.index(@norm_data.max)+1], 1, max_color) if has_max == true
    
    drawbox(coords[-2], 1, last_color) if has_last == true

    @draw.draw(@canvas)
    @canvas.to_blob
  end

  ##
  # A bar graph.

  def bar
    step = @options[:step].to_i
    height = @options[:height].to_f
    background_color = @options[:background_color]

    create_canvas(@norm_data.length * step + 2, height, background_color)
    
    upper = @options[:upper].to_i
    below_color = @options[:below_color]
    above_color = @options[:above_color]

    i = 1
    @norm_data.each_with_index do |r, index|
      color = (r >= upper) ? above_color : below_color
      @draw.stroke('transparent')
      @draw.fill(color)
      @draw.rectangle( i, @canvas.rows, 
	    i + step - 2, @canvas.rows - ( (r / @maximum_value) * @canvas.rows) )
      i += step
    end

    @draw.draw(@canvas)
    @canvas.to_blob
  end


  ##
  # Creates a discretized sparkline
  #
  # :height - An integer that determines what the height of the sparkline will be.  Defaults to 14
  #
  # :upper - An integer that determines the threshold for colorization purposes.  Any value less than upper will be colored using below_color, anything above and equal to upper will use above_color.  Defaults to 50.
  #
  # :above_color - A string or color code representing the color to draw values above or equal the upper value.  Defaults to red.
  #
  # :below_color - A string or color code representing the color to draw values below the upper value. Defaults to gray.

  def discrete

    height = @options[:height].to_i
    upper = @options[:upper].to_i
    background_color = @options[:background_color]
    step = @options[:step].to_i
        
    create_canvas(@norm_data.size * step - 1, height, background_color)
    
    below_color = @options[:below_color]
    above_color = @options[:above_color]

    i = 0
    @norm_data.each do |r|
    	color = (r >= upper) ? above_color : below_color
    	@draw.stroke(color)
    	@draw.line(i, (@canvas.rows - r/(101.0/(height-4))-4).to_i,
    	          i, (@canvas.rows - r/(101.0/(height-4))).to_i)
    	i += step
    end

    @draw.draw(@canvas)
    @canvas.to_blob
  end


  ##
  # Creates a pie-chart sparkline
  #
  # :diameter - An integer that determines what the size of the sparkline will be.  Defaults to 20
  #
  # :share_color - A string or color code representing the color to draw the share of the pie represented by percent.  Defaults to red.
  #
  # :remain_color - A string or color code representing the color to draw the pie not taken by the share color. Defaults to lightgrey.

  def pie
    diameter = @options[:diameter].to_i
    background_color = @options[:background_color]

    create_canvas(diameter, diameter, background_color)
    
    share_color = @options[:share_color]
    remain_color = @options[:remain_color]
    percent = @norm_data[0]
    
    # Adjust the radius so there's some edge left in the pie
    r = diameter/2.0 - 2
    @draw.fill(remain_color)
    @draw.ellipse(r + 2, r + 2, r , r , 0, 360)
    @draw.fill(share_color)

    # Special exceptions
    if percent == 0
    	# For 0% return blank
    	@draw.draw(@canvas)
    	return @canvas.to_blob
    elsif percent == 100
    	# For 100% just draw a full circle
    	@draw.ellipse(r + 2, r + 2, r , r , 0, 360)
    	@draw.draw(@canvas)
    	return @canvas.to_blob
    end

    # Okay, this part is as confusing as hell, so pay attention:
    # This line determines the horizontal portion of the point on the circle where the X-Axis
    # should end.  It's caculated by taking the center of the on-image circle and adding that
    # to the radius multiplied by the formula for determinig the point on a unit circle that a
    # angle corresponds to.  3.6 * percent gives us that angle, but it's in degrees, so we need to
    # convert, hence the muliplication by Pi over 180
    arc_end_x = r + 2 + (r * Math.cos((3.6 * percent)*(Math::PI/180)))

    # The same goes for here, except it's the vertical point instead of the horizontal one
    arc_end_y = r + 2 + (r * Math.sin((3.6 * percent)*(Math::PI/180)))

    # Because the SVG path format is seriously screwy, we need to set the large-arc-flag to 1
    # if the angle of an arc is greater than 180 degrees.  I have no idea why this is, but it is.
    percent > 50? large_arc_flag = 1: large_arc_flag = 0

    # This is also confusing
    # M tells us to move to an absolute point on the image.  We're moving to the center of the pie
    # h tells us to move to a relative point.  We're moving to the right edge of the circle.
    # A tells us to start an absolute elliptical arc.  The first two values are the radii of the ellipse
    # the third value is the x-axis-rotation (how to rotate the ellipse if we wanted to [could have some fun
    # with randomizing that maybe), the fourth value is our large-arc-flag, the fifth is the sweep-flag,
    # (again, confusing), the sixth and seventh values are the end point of the arc which we calculated previously
    # More info on the SVG path string format at: http://www.w3.org/TR/SVG/paths.html
    path = "M#{r + 2},#{r + 2} h#{r} A#{r},#{r} 0 #{large_arc_flag},1 #{arc_end_x},#{arc_end_y} z"
    @draw.path(path)

    @draw.draw(@canvas)
    @canvas.to_blob
  end


  ##
  # Creates a smooth sparkline.
  #
  # :step - An integer that determines the distance between each point on the sparkline.  Defaults to 2.
  #
  # :height - An integer that determines what the height of the sparkline will be.  Defaults to 14
  #
  # :has_min - Determines whether a dot will be drawn at the lowest value or not.  Defaults to false.
  #
  # :has_max - Determines whether a dot will be drawn at the highest value or not.  Defaults to false.
  #
  # :has_last - Determines whether a dot will be drawn at the last value or not.  Defaults to false.
  #
  # :min_color - A string or color code representing the color that the dot drawn at the smallest value will be displayed as.  Defaults to blue.
  #
  # :max_color - A string or color code representing the color that the dot drawn at the largest value will be displayed as.  Defaults to green.
  #
  # :last_color - A string or color code representing the color that the dot drawn at the last value will be displayed as.  Defaults to red.

  def smooth

    step = @options[:step].to_i
    height = @options[:height].to_i
    background_color = @options[:background_color]

    create_canvas((@norm_data.size - 1) * step + 4, height, background_color)
    
    min_color = @options[:min_color]
    max_color = @options[:max_color]
    last_color = @options[:last_color]
    has_min = @options[:has_min]
    has_max = @options[:has_max]
    has_last = @options[:has_last]
    line_color = @options[:line_color]

    @draw.stroke(line_color)
    coords = []
    i=0
    @norm_data.each do |r|
      coords.push [ 2 + i, (height - 3 - r/(101.0/(height-4))) ]
      i += step
    end
    
    open_ended_polyline(coords)

    drawbox(coords[@norm_data.index(@norm_data.min)], 2, min_color) if has_min == true

    drawbox(coords[@norm_data.index(@norm_data.max)], 2, max_color) if has_max == true

    drawbox(coords[-1], 2, last_color) if has_last == true

    @draw.draw(@canvas)
    @canvas.to_blob
  end


  ##
  # Creates a whisker sparkline to track on/off type data. There are five states: 
  # on, off, no value, exceptional on, exceptional off. On values create an up 
  # whisker and off values create a down whisker. Exceptional values may be 
  # colored differently than regular values to indicate, for example, a shut out.
  # No value produces an empty row to indicate a tie.
  # 
  # * results - an array of integer values between -2 and 2. -2 is exceptional 
  #   down, 1 is regular down, 0 is no value, 1 is up, and 2 is exceptional up.
  # * options - a hash that takes parameters
  #
  # :height - height of the sparkline
  #
  # :whisker_color - the color of regular whiskers; defaults to black
  #
  # :exception_color - the color of exceptional whiskers; defaults to red
  
  def whisker

    # step = @options[:step].to_i
    height = @options[:height].to_i
    background_color = @options[:background_color]

    create_canvas((@data.size - 1) * 2, height, background_color)
    
    whisker_color = @options[:whisker_color] || 'black'
    exception_color = @options[:exception_color] || 'red'

    i = 0
    @data.each do |r|
      color = whisker_color

      if ( (r == 2 || r == -2) && exception_color )
        color = exception_color
      end

      y_mid_point = (r >= 1) ? (@canvas.rows/2.0 - 1).ceil : (@canvas.rows/2.0).floor

      y_end_point = y_mid_point
      if ( r > 0) 
        y_end_point = 0
      end

      if ( r < 0 )
        y_end_point = @canvas.rows
      end

      @draw.stroke( color )
      @draw.line( i, y_mid_point, i, y_end_point )
      i += 2
    end

    @draw.draw(@canvas)
    @canvas.to_blob 
  end

  ##
  # Draw the error Sparkline.

  def plot_error(options={})
    create_canvas(40, 15, 'white')

    @draw.fill('red')
    @draw.line(0,0,40,15)
    @draw.line(0,15,40,0)

    @draw.draw(@canvas)
    @canvas.to_blob
  end

private

  def normalize_data
    @maximum_value = @data.max
    if @options[:type].to_s == 'pie'
      @norm_data = @data
    else
      @norm_data = @data.map { |value| value = (value.to_f / @maximum_value) * 100.0 }
    end
  end

  ##
  # * :arr - an array of points (represented as two element arrays)
  
  def open_ended_polyline(arr)
    0.upto(arr.length - 2) { |i|
      @draw.line(arr[i][0], arr[i][1], arr[i+1][0], arr[i+1][1])
    }
  end

  ##
  # Create an image to draw on and a drawable to do the drawing with.
  #
  # TODO Refactor into smaller methods

  def create_canvas(w, h, bkg_col)
    @draw = Magick::Draw.new
    @draw.pointsize = @@pointsize # TODO Use height
    @canvas = Magick::Image.new(w , h) { self.background_color = bkg_col }

    # Make room for label and last value
    unless @options[:label].nil?
      @options[:has_last] = true
      @label_width = calculate_width(@options[:label])
      @data_last_width = calculate_width(@data.last)
      # HACK The 7.0 is a severe hack. Must figure out correct spacing
      @label_and_data_last_width = @label_width + @data_last_width + @@label_margin * 7.0
      w += @label_and_data_last_width
    end

    @canvas = Magick::Image.new(w , h) { self.background_color = bkg_col }
    @canvas.format = "PNG"

    # Draw label and last value
    unless @options[:label].nil?
      if ENV.has_key?('MAGICK_FONT_PATH')
        vera_font_path = File.expand_path('Vera.ttf', ENV['MAGICK_FONT_PATH'])
        @font = File.exists?(vera_font_path) ? vera_font_path : nil
      else
        @font = nil
      end

      @draw.fill = 'black'
      @draw.font = @font if @font
      @draw.gravity = Magick::WestGravity
      @draw.annotate( @canvas, 
                      @label_width, 1.0,
                      w - @label_and_data_last_width + @@label_margin, h - calculate_caps_height/2.0,
                      @options[:label])

      @draw.fill = 'red'
      @draw.annotate( @canvas, 
                      @data_last_width, 1.0,
                      w - @data_last_width - @@label_margin * 2.0, h - calculate_caps_height/2.0,
                      @data.last.to_s)
    end
  end

  ##
  # Utility to draw a coloured box
  # Centred on pt, offset off in each direction, fill color is col

  def drawbox(pt, offset, color)
    @draw.stroke 'transparent'
    @draw.fill(color)
    @draw.rectangle(pt[0]-offset, pt[1]-offset, pt[0]+offset, pt[1]+offset)
  end

  def calculate_width(text)
    @draw.get_type_metrics(@canvas, text.to_s).width
  end

  def calculate_caps_height
    @draw.get_type_metrics(@canvas, 'X').height
  end

end

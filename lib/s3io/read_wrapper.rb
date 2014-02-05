module S3io

  def self.reader(s3object, options = {}, &block)
    open(s3object, 'r', options, &block)
  end

  class ReadWrapper < Wrapper

    # Default buffer size for line parser in bytes
    LINE_BUFFER_SIZE = 5 * 1024 * 1024 # MiB

    include Enumerable

    # Current byte position in S3 object
    attr_accessor :pos

    def initialize(s3object, options = {})
      super(s3object)

      @options = {
        :line_buffer_size => (options[:line_buffer_size] || LINE_BUFFER_SIZE)
      }
    end

    # Reads data from S3 object.
    #
    # @param [Integer] bytes number of bytes to read
    def read(bytes = nil)
      content_length = @s3object.content_length

      return '' if (@pos >= content_length) || (bytes == 0)

      bytes ||= content_length

      upper_bound = @pos + bytes - 1
      upper_bound = (content_length - 1) if upper_bound >= content_length

      data = @s3object.read :range => @pos..upper_bound
      @pos = upper_bound + 1

      return data
    end

    def eof?
      @pos >= @s3object.content_length
    end

    # Rewinds position to the very beginning of S3 object.
    def rewind
      @pos = 0
    end

    # Iterates over S3 object lines.
    #
    # @param [String] separator line separator string
    def each(separator = $/)
      return enum_for(:each, separator) unless block_given?

      line = ''
      newline_pos = nil

      # Either trying to parse the remainder or reading some more data
      while newline_pos || !(buffer = read(@options[:line_buffer_size])).empty?
        prev_newline_pos = newline_pos || 0
        newline_pos = buffer.index(separator, prev_newline_pos)

        if newline_pos
          line << buffer[prev_newline_pos..newline_pos]
          newline_pos += 1
          yield line
          line = ''
        else
          line << buffer[prev_newline_pos..-1]
        end
      end

      # Flush the remainder if body doesn't end with separator
      yield line unless line.empty?

      return self
    end
    alias lines each
    alias each_line each

    # Returns strings line by line.  Optional params are a separator and length limit.
    def gets(*args)
      if args.count == 0
        separator = $/
        limit = nil
      elsif args.count == 1 && args[0].kind_of?(Integer)
        separator = $/
        limit = args[0]
      elsif args.count == 1
        separator = args[0] ? args[0] : $/
        limit = nil
      elsif args.count == 2
        separator = args[0] ? args[0] : $/
        limit = args[1]
      else
        raise ArgumentError.new('Incorrect number of  arguments')
      end
      @_gets ||= enum_for(:each, separator)
      limit ? @_gets.next.byteslice(0, limit) : @_gets.next
    rescue StopIteration
      nil
    end

  end
end

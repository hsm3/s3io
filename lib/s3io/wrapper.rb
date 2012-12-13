module S3io
  # This class wraps an AWS S3 object in order to provide IO-like API.
  # S3 objects wrapped this way can be used in methods that would otherwise expect an instance of File, StringIO etc.
  class Wrapper

    # Default buffer size for line parser in bytes
    LINE_BUFFER_SIZE = 5 * 1024 * 1024 # MiB

    include Enumerable

    # Current byte position in S3 object
    attr_accessor :pos

    # Options that were passed during initialization
    attr_reader :options

    # Wraps an AWS::S3::S3Object into IO-like object.
    #
    # @param [AWS::S3::S3Object] s3object an object to wrap
    # @param [Hash] options options hash
    # @option options [Integer] :line_buffer_size size of the buffer that is used for reading contents of S3 object when iterating over its lines
    def initialize(s3object, options = {})
      @s3object = s3object
      @options = {
        :line_buffer_size => (options[:line_buffer_size] || LINE_BUFFER_SIZE)
      }

      @pos = 0
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

  end
end

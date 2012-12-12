module S3io
  class Wrapper

    LINE_BUFFER_SIZE = 5 * 1024 * 1024 # MiB

    include Enumerable

    attr_accessor :pos
    attr_reader :options

    def initialize(s3object, options = {})
      @s3object = s3object
      @options = {
        :line_buffer_size => (options[:line_buffer_size] || LINE_BUFFER_SIZE)
      }

      @pos = 0
    end

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

    def rewind
      @pos = 0
    end

    def each(separator = $/)
      return enum_for(:each, separator) unless block_given?

      line = ''
      newline_pos = nil

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

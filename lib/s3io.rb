require "s3io/version"
require "s3io/wrapper"
require "s3io/read_wrapper"
require "s3io/write_wrapper"

require "aws-sdk"

# A top-level module that provides an S3 wrapper class.
module S3io
  # A shortcut for wrapping an S3 object
  #
  # @param [AWS::S3::S3Object] s3object an object to wrap
  # @param [Hash] options options hash
  # @option options [Integer] :line_buffer_size size of the buffer that is used for reading contents of S3 object when iterating over its lines
  # @return [S3io::Wrapper] a wrapped S3 object
  #def self.new(s3object, options = {})
  #  Wrapper.new(s3object, options)
  #end
end

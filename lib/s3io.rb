require "s3io/version"
require "s3io/wrapper"

require "aws-sdk"

module S3io
  def self.new(s3object, options = {})
    Wrapper.new(s3object, options)
  end
end

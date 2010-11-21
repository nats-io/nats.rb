if RUBY_VERSION <= "1.8.6"
  class String #:nodoc:
    def bytesize; self.size; end
  end
end

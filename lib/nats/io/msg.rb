# Copyright 2016-2021 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
require_relative 'js'

module NATS
  class Msg
    attr_accessor :subject, :reply, :data, :header

    # Enhance it with ack related methods from JetStream to ack msgs.
    include JetStream::Msg::AckMethods

    def initialize(opts={})
      @subject = opts[:subject]
      @reply   = opts[:reply]
      @data    = opts[:data]
      @header  = opts[:header]
      @nc      = opts[:nc]
      @sub     = opts[:sub]
      @ackd    = false
      @meta    = nil
    end

    def respond(data)
      return unless @nc
      @nc.publish(self.reply, data)
    end

    def respond_msg(msg)
      return unless @nc
      @nc.publish_msg(msg)
    end

    def inspect
      hdr = ", header=#{@header}" if @header
      "#<NATS::Msg(subject: \"#{@subject}\", reply: \"#{@reply}\", data: #{@data.slice(0, 10).inspect}#{hdr})>"
    end
  end
end

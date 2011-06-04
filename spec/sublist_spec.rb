require 'spec_helper'
require 'nats/server/sublist'

describe 'sublist functionality' do

  before do
    @sublist = Sublist.new
  end

  it 'should be empty on start' do
    @sublist.count.should == 0
    @sublist.match('a').should be_empty
    @sublist.match('a.b').should be_empty
  end

  it 'should have N items after N items inserted' do
    5.times { |n| @sublist.insert("a.b.#{n}", 'foo') }
    @sublist.count.should == 5
  end

  it 'should be safe to remove an non-existant subscription' do
    @sublist.insert('a.b.c', 'foo')
    @sublist.remove('a.b.x', 'foo')
  end

  it 'should have 0 items after N items inserted and removed' do
    5.times { |n| @sublist.insert("a.b.#{n}", 'foo') }
    @sublist.count.should == 5
    5.times { |n| @sublist.remove("a.b.#{n}", 'foo') }
    @sublist.count.should == 0
  end

  it 'should have N items after N items inserted and others removed' do
    5.times { |n| @sublist.insert("a.b.#{n}", 'foo') }
    @sublist.count.should == 5
    @sublist.remove('a.b.c', 'foo')
    @sublist.count.should == 5
  end

  it 'should match and return proper closure for subjects' do
    @sublist.insert('a', 'foo')
    m = @sublist.match('a')
    m.should_not be_empty
    m.size.should == 1
    m[0].should == 'foo'
  end

  it 'should not match after the item has been removed' do
    @sublist.insert('a', 'foo')
    @sublist.remove('a', 'foo')
    @sublist.match('a').should be_empty
  end

  it 'should match simple multiple tokens' do
    @sublist.insert('a.b.c', 'foo')
    m = @sublist.match('a.b.c')
    m.should_not be_empty
    m.size.should == 1
    m[0].should == 'foo'
    m = @sublist.match('a.b.z')
    m.should be_empty
  end

  it 'should match full wildcards on any proper subject' do
    @sublist.insert('a.b.>', 'foo')
    m = @sublist.match('a.b.c')
    m.should_not be_empty
    m.size.should == 1
    m[0].should == 'foo'
    m = @sublist.match('a.b.z')
    m.should_not be_empty
    m.size.should == 1
    m[0].should == 'foo'
    m = @sublist.match('a.b.c.d.e.f')
    m.should_not be_empty
    m.size.should == 1
    m[0].should == 'foo'
  end

  it 'should match positional wildcards on any proper subject' do
    @sublist.insert('a.*.c', 'foo')
    m = @sublist.match('a.b.c')
    m.should_not be_empty
    m.size.should == 1
    m[0].should == 'foo'
    m = @sublist.match('a.b.z')
    m.should be_empty
    m = @sublist.match('a.z.c')
    m.should_not be_empty
    m.size.should == 1
    m[0].should == 'foo'
  end

  it 'should properly match multiple wildcards' do
    @sublist.insert('*.b.*', '2pwc')
    m = @sublist.match('a.b.c')
    m.count.should == 1
    m.should == ['2pwc']
    @sublist.insert('a.>', 'fwc')
    m = @sublist.match('a.z.c')
    m.count.should == 1
    m.should == ['fwc']
    m = @sublist.match('a.b.c.d')
    m.count.should == 1
    m.should == ['fwc']
  end

  it 'should properly mix and match simple, fwc, and pwc wildcards' do
    @sublist.insert('a.b.c', 'simple')
    @sublist.insert('a.b.>', 'fwc')
    @sublist.insert('a.*.c', 'pwc-middle')
    @sublist.insert('*.b.c', 'pwc-first')
    @sublist.insert('a.b.*', 'pwc-last')
    m = @sublist.match('a.b.c').sort
    m.count.should == 5
    m.should == ['fwc', 'pwc-first', 'pwc-last', 'pwc-middle', 'simple']
    m = @sublist.match('a.b.c.d.e.f')
    m.count.should == 1
    m.should == ['fwc']
    m = @sublist.match('z.b.c')
    m.count.should == 1
    m.should == ['pwc-first']
    m = @sublist.match('a.z.c')
    m.count.should == 1
    m.should == ['pwc-middle']
    m = @sublist.match('a.b.z').sort
    m.count.should == 2
    m.should == ['fwc', 'pwc-last']
  end

  it 'should have N node_count after N items inserted' do
    5.times { |n| @sublist.insert("a.b.#{n}", 'foo') }
    nc = @sublist.send :node_count
    nc.should == 7
  end

  it 'should have 0 node_count after N items inserted and removed' do
    5.times { |n| @sublist.insert("#{n}", 'foo') }
    5.times { |n| @sublist.remove("#{n}", 'foo') }
    nc = @sublist.send :node_count
    nc.should == 0
  end

  it 'should have 0 node_count after N items with 1 token prefix inserted and removed' do
    5.times { |n| @sublist.insert("INBOX.#{n}", 'foo') }
    5.times { |n| @sublist.remove("INBOX.#{n}", 'foo') }
    nc = @sublist.send :node_count
    nc.should == 0
  end

  it 'should have 0 node_count after N items with 3 prefix and wildcards inserted and removed' do
    5.times { |n| @sublist.insert("a.b.*.#{n}", 'foo') }
    @sublist.insert('a.b.>', 'foo')
    5.times { |n| @sublist.remove("a.b.*.#{n}", 'foo') }
    @sublist.remove('a.b.>', 'foo')
    nc = @sublist.send :node_count
    nc.should == 0
  end

  it 'should have 0 node_count after N items with large middle token range inserted and removed' do
    5.times { |n| @sublist.insert("a.#{n}.c", 'foo') }
    5.times { |n| @sublist.remove("a.#{n}.c", 'foo') }
    nc = @sublist.send :node_count
    nc.should == 0
  end

end

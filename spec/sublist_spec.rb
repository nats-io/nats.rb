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

end

#--
#
# Sublist implementation for a publish-subscribe system.
# This container class holds subscriptions and matches
# candidate subjects to those subscriptions.
# Certain wildcards are supported for subscriptions.
# '*' will match any given token at any level.
# '>' will match all subsequent tokens.
#--
# See included test for example usage:
##

class Sublist #:nodoc:
  PWC = '*'.freeze
  FWC = '>'.freeze
  CACHE_SIZE = 4096

  attr_reader :count

  SublistNode  = Struct.new(:leaf_nodes, :next_level)
  SublistLevel = Struct.new(:nodes, :pwc, :fwc)

  EMPTY_LEVEL = SublistLevel.new({})

  def initialize(options = {})
    @count = 0
    @results = []
    @root = SublistLevel.new({})
    @cache = {}
  end

  # Ruby is a great language to make selective trade offs of space versus time.
  # We do that here with a low tech front end cache. The cache holds results
  # until it is exhausted or if the instance inserts or removes a subscription.
  # The assumption is that the cache is best suited for high speed matching,
  # and that once it is cleared out it will naturally fill with the high speed
  # matches. This can obviously be improved with a smarter LRU structure that
  # does not need to completely go away when a remove happens..
  #
  # front end caching is on by default, but we can turn it off here if needed

  def disable_cache; @cache = nil; end
  def enable_cache;  @cache ||= {};  end
  def clear_cache; @cache = {} if @cache; end

  # Random removal
  def prune_cache
    return unless @cache
    keys = @cache.keys
    @cache.delete(keys[rand(keys.size)])
  end

  # Insert a subscriber into the sublist for the given subject.
  def insert(subject, subscriber)
    # TODO - validate subject as correct.
    level, tokens = @root, subject.split('.')
    for token in tokens
      # This is slightly slower than direct if statements, but looks cleaner.
      case token
        when FWC then node = (level.fwc || (level.fwc = SublistNode.new([])))
        when PWC then node = (level.pwc || (level.pwc = SublistNode.new([])))
        else node  = ((level.nodes[token]) || (level.nodes[token] = SublistNode.new([])))
      end
      level = (node.next_level || (node.next_level = SublistLevel.new({})))
    end
    node.leaf_nodes.push(subscriber)
    @count += 1
    clear_cache # Clear the cache
    node.next_level = nil if node.next_level == EMPTY_LEVEL
  end

  # Remove a given subscriber from the sublist for the given subject.
  def remove(subject, subscriber)
    return unless subject && subscriber
    remove_level(@root, subject.split('.'), subscriber)
  end

  # Match a subject to all subscribers, return the array of matches.
  def match(subject)
    return @cache[subject] if (@cache && @cache[subject])
    tokens = subject.split('.')
    @results.clear
    matchAll(@root, tokens)
    # FIXME: This is too low tech, will revisit when needed.
    if @cache
      prune_cache if @cache.size > CACHE_SIZE
      @cache[subject] = Array.new(@results).freeze # Avoid tampering of copy
    end
    @results
  end

  private

  def matchAll(level, tokens)
    node, pwc = nil, nil # Define for scope
    i, ts = 0, tokens.size
    while (i < ts) do
      return if level == nil
      # Handle a full wildcard here by adding all of the subscribers.
      @results.concat(level.fwc.leaf_nodes) if level.fwc
      # Handle an internal partial wildcard by branching recursively
      lpwc = level.pwc
      matchAll(lpwc.next_level, tokens[i+1, ts]) if lpwc
      node, pwc = level.nodes[tokens[i]], lpwc
      #level = node.next_level if node
      level = node ? node.next_level : nil
      i += 1
    end
    @results.concat(pwc.leaf_nodes) if pwc
    @results.concat(node.leaf_nodes) if node
  end

  def prune_level(level, node, token)
    # Prune here if needed.
    return unless level && node
    return unless node.leaf_nodes.empty? && (!node.next_level || node.next_level == EMPTY_LEVEL)
    if node == level.fwc
      level.fwc = nil
    elsif node == level.pwc
      level.pwc = nil
    else
      level.nodes.delete(token)
    end
  end

  def remove_level(level, tokens, subscriber)
    return unless level
    token = tokens.shift
    case token
      when FWC then node = level.fwc
      when PWC then node = level.pwc
      else node = level.nodes[token]
    end
    return unless node

    # This could be expensive if a large number of subscribers exist.
    if tokens.empty?
      if (node.leaf_nodes && node.leaf_nodes.delete(subscriber))
        @count -= 1
        prune_level(level, node, token)
        clear_cache # Clear the cache
      end
    else
      remove_level(node.next_level, tokens, subscriber)
      prune_level(level, node, token)
    end
  end

  ################################################
  # Used for tests on pruning subscription nodes.
  ################################################

  def node_count_level(level, nc)
    return 0 unless level
    nc += 1 if level.fwc
    nc += node_count_level(level.pwc.next_level, nc+1) if level.pwc
    level.nodes.each_value do |node|
      nc += node_count_level(node.next_level, nc)
    end
    nc += level.nodes.length
  end

  def node_count
    node_count_level(@root, 0)
  end

end

class SimpleArrayIndex
  def initialize
    @index = Array(UInt64).new
  end

  def add(id, offset)
    @index << offset
  end

  def find(id)
    @index[id-1]
  end

  def last
    last = @index.last?
    return {@index.size.to_u64, last} if !last.nil?
    return {0_u64, 0_u64}
  end
end

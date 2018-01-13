class CompressedSparseIndex
  def initialize(@density : Int32)
    @a4x4 = Array(Tuple(UInt32, UInt32)).new
    @a4x8 = Array(Tuple(UInt32, UInt64)).new
    @a8x8 = Array(Tuple(UInt64, UInt64)).new
  end

  def add(id, offset)
    return if id % @density != 1
    id_byte_count = used_bytes(id)
    offset_byte_count = used_bytes(offset)
    if id_byte_count <= 4 && offset_byte_count <= 4
      @a4x4.push({id.to_u32, offset.to_u32})
    elsif id_byte_count <= 4
      @a4x8.push({id.to_u32, offset})
    else
      @a8x8.push({id, offset})
    end
  end

  def find(id)
    found = find_in(id, @a4x4)
    return found if found > 0
    found = find_in(id, @a4x8)
    return found if found > 0
    return find_in(id, @a8x8)
  end

  def last
    last = @a4x8.last?
    return last if !last.nil?
    last = @a4x8.last?
    return {last[0].to_u64, last[1]} if !last.nil?
    last = @a4x4.last?
    return {last[0].to_u64, last[1].to_u64} if !last.nil?
    return {0_u64, 0_u64}
  end

  private def find_in(id, array)
    i = array.bsearch_index { |a| a[0] >= id }
    return array[i][1] if i && array[i][0] == id
    return array[i-1][1] if i && i - 1 >= 0
    return array[array.size-1][1] if array.size > 0
    return 0_u64
  end

  private def used_bytes(a : UInt64)
    bytes = StaticArray(UInt8, 8).new(0_u8).to_slice
    IO::ByteFormat::LittleEndian.encode(a, bytes)
    non_zero_count = 0
    bytes.each { |byte| non_zero_count += 1 if byte != 0 }
    i = 7_u8
    while i > 0
      break if bytes[i] != 0
      i -= 1
    end
    return i
  end
end

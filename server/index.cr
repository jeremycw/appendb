class Index
  def initialize(@id : UInt32)
    @fmt = IO::ByteFormat::LittleEndian
    @density = 10_u16
    @file = File.open("#{Dir.current}/#{@id}.idx", "a+")
    @index = Array(Tuple(UInt64, UInt64)).new
    if @file.size > 0
      bytes_read = 0_u64
      while bytes_read < @file.size
        id = @file.read_bytes(UInt64, @fmt)
        offset = @file.read_bytes(UInt64, @fmt)
        bytes_read += sizeof(typeof(id)) + sizeof(typeof(offset))
        @index << {id, offset}
      end
    end
  end

  def find(id)
    i = @index.bsearch_index { |a| a[0] >= id }
    return @index[i][1] if i && @index[i][0] == id
    return @index[i-1][1] if i && i - 1 >= 0
    return @index[@index.size-1][1] if @index.size > 0
    return 0_u64
  end

  def add(id, offset)
    return if id % @density != 1
    @index.push({id, offset})
    @file.seek(0, IO::Seek::End)
    @file.write_bytes(id, @fmt)
    @file.write_bytes(offset, @fmt)
    @file.flush
  end

  def last
    @index.last? || {0_u64, 0_u64}
  end

  def close
    @file.close
  end
end


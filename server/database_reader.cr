class DatabaseReader
  def initialize(@file : CachedFile, @index : Index)
    @fmt = IO::ByteFormat::LittleEndian
  end

  def seek_to(id)
    offset = @index.find(id)
    @file.seek(offset, IO::Seek::Set)
    loop do
      found = @file.read_bytes(UInt64, @fmt)
      break if found == id
      size = @file.read_bytes(UInt16, @fmt)
      @file.seek(@file.pos + size, IO::Seek::Set)
    end
    @file.seek(@file.pos - sizeof(typeof(id)), IO::Seek::Set)
  end

  def last_id
    return 0_u64 if @file.size == 0
    @index.last[0]
  end

  def bytes_until_eof
    @file.size - @file.pos
  end

  def read_into(io)
    IO.copy(@file, io)
  end

  def close
    @file.close
  end
end

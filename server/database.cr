class Database
  @autoinc : UInt64

  def initialize(@id : UInt32)
    filename = "#{Dir.current}/#{@id}.dat"
    if !File.exists?(filename)
      File.touch(filename)
    end
    @db = File.open(filename, "a+")
    @fmt = IO::ByteFormat::LittleEndian
    @index = Index.new(@id)
    @autoinc = reader.last_id
  end

  def append(client)
    size = client.read_bytes(UInt16, @fmt)
    @autoinc += 1
    @index.add(@autoinc, @db.size)
    @db.write_bytes(@autoinc, @fmt)
    @db.write_bytes(size, @fmt)
    IO.copy(client, @db, size)
    @db.flush
    return @autoinc
  end

  def reader
    DatabaseReader.new(@id, @index)
  end

  def close
    @db.close
    @index.close
  end
end

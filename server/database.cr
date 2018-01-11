class Database

  def initialize(@id : UInt32)
    @fmt = IO::ByteFormat::LittleEndian
    filename = "#{Dir.current}/#{@id}.dat"
    File.touch(filename) if !File.exists?(filename)
    @index = Index.new(@id)
    @in = Channel(IO | Nil).new
    @out = Channel(UInt64).new
    spawn writer()
  end

  def append(client)
    @in.send(client)
    return @out.receive
  end

  def reader
    DatabaseReader.new(@id, @index)
  end

  def close
    @in.send(nil)
    @index.close
  end

  private def writer()
    reader = self.reader
    autoinc = reader.last_id
    reader.close
    file = get_consistent_file
    loop do
      client = @in.receive
      break if client.is_a?(Nil)
      size = client.read_bytes(UInt16, @fmt)
      autoinc += 1
      @index.add(autoinc, file.size)
      file.write_bytes(autoinc, @fmt)
      file.write_bytes(size, @fmt)
      IO.copy(client, file, size)
      file.flush
      @out.send(autoinc)
    end
    file.close
  end

  private def get_consistent_file
    filename = "#{Dir.current}/#{@id}.dat"
    File.open(filename, "a+")
  end
end

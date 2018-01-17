class Cache < IO
  def initialize(@block_size : UInt32, @max_blocks : UInt32)
    @pos = 0_i64
    @storage = Deque(Slice(UInt8)).new
    @storage << Slice(UInt8).new(@block_size, 0_u8)
    @size = 0_i64
  end

  property pos
  getter size

  def seek(offset : Int64, whence : IO::Seek = IO::Seek::Set)
    case whence
    when .set?
      @pos = offset
    when .end?
      @pos = @size + offset
    when .current?
      @pos += offset
    end
  end

  def write(slice : Bytes)
    slice.each do |b|
      i = @pos / @block_size
      if i >= @storage.size
        if @storage.size >= @max_blocks
          @storage.rotate!
        else
          @storage << Slice(UInt8).new(@block_size.to_i32, 0_u8)
        end
      end
      block = @storage[i]
      block[@pos % @block_size] = b
      @pos += 1
    end
    @size += slice.size
    nil
  end

  def read(slice : Bytes)
    idx = 0
    slice.size.times do
      raise IO::EOFError.new if @pos >= @size
      i = @pos / @block_size
      block = @storage[i]
      slice[idx] = block[@pos % @block_size]
      @pos += 1
      idx += 1
    end
    slice.size
  end

end

class CachedFile < IO

  @cache_offset : UInt64

  delegate pos, seek, flush, close, size, truncate, to: @file

  def self.open(path, size = 134217728_u64)
    file = File.open(path, "a+")
    self.new(file, size)
  end

  protected def initialize(@file : File, @cache_max_size : UInt64)
    @cache = IO::Memory.new
    @cache_offset = @file.size
  end

  protected def initialize(@file : File, @cache_max_size : UInt64, @cache : IO, @cache_offset : UInt64)
  end

  def write(slice : Bytes)
    @cache.seek(0, IO::Seek::End)
    @cache.write(slice)
    @file.write(slice)
  end

  def read(slice : Bytes)
    if @file.pos >= @cache_offset
      @cache.seek(@file.pos - @cache_offset)
      @file.pos += slice.size
      return @cache.read(slice)
    else
      return @file.read(slice)
    end
  end

  def readonly_copy
    file = File.open(@file.path, "r")
    CachedFile.new(file, @cache_max_size, @cache, @cache_offset)
  end
end

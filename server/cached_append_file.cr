require "./caches/ring_buffer.cr"

class CachedFile < IO

  @cache_offset : UInt64

  delegate pos, seek, flush, close, size, truncate, to: @file

  def self.open(path, size = 4096_u64)
    file = File.open(path, "a+")
    self.new(file, size)
  end

  protected def initialize(@file : File, @cache_max_size : UInt64)
    @cache = RingBuffer.new(@cache_max_size.to_i32)
    @cache_offset = @file.size
  end

  protected def initialize(@file : File, @cache_max_size : UInt64, @cache : Cache, @cache_offset : UInt64)
  end

  def write(slice : Bytes)
    @cache.seek(0, IO::Seek::End)
    @cache.write(slice)
    @file.write(slice)
  end

  def read(slice : Bytes)
    if @file.pos >= @cache.tail + @cache_offset
      @cache.seek(@file.pos - @cache_offset)
      read = @cache.read(slice)
      @file.pos += read
      return read
    else
      read = @file.read(slice)
      return read
    end
  end

  def truncate(size)
    @file.truncate(size)
    @cache.seek(size, IO::Seek::Set)
  end

  def readonly_copy
    file = File.open(@file.path, "r")
    CachedFile.new(file, @cache_max_size, @cache, @cache_offset)
  end
end

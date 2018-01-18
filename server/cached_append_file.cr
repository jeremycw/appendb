require "./caches/slab_ring_buffer.cr"
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

  protected def initialize(@file : File, @cache_max_size : UInt64, @cache : IO, @cache_offset : UInt64)
  end

  def write(slice : Bytes)
    STDOUT.puts "writing"
    @cache.seek(0, IO::Seek::End)
    @cache.write(slice)
    @file.write(slice)
  end

  def read(slice : Bytes)
    STDOUT.puts "reading: #{@file.pos}, #{@cache_offset}"
    if @file.pos >= @cache_offset
      STDOUT.puts "read attempt"
      @cache.seek(@file.pos - @cache_offset)
      read = @cache.read(slice)
      STDOUT.puts "read: #{read}"
      @file.pos += read
      return read
    else
      STDOUT.puts "fread attempt"
      read = @file.read(slice)
      STDOUT.puts "fread: #{read}"
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

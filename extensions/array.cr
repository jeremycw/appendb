class Array(T)
  def truncate(size)
    raise "Truncation size must be smaller than current size" if size > @size
    raise "Truncation size must be positive" if size < 0
    @size = size
  end
end

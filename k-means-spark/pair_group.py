class PairGroup:
  def __init__(self, focus, pairs):
    self.focus = focus
    self.pairs = pairs

  def sum(self):
    sum = 0
    for pair in self.pairs:
      sum += pair.distance
    return sum

  def __str__(self):
    return "Focus " + str(self.focus) + " with " + str(self.pairs)

  def __repr__(self):
      # Spark uses this method when save on text file
      return str(self)
import numpy as np

def inc(pair, bar):
  pair.distance += bar

def dec(pair, bar):
  pair.distance -= bar

def foo(bar):
  incr = lambda pair: inc(pair, bar)
  decr = lambda pair: dec(pair, bar)
  return (incr, decr)

class PairGroup:
  def __init__(self, focus, pairs):
    self.focus = focus
    self.pairs = pairs

  def sum(self):
    sum = 0
    for pair in self.pairs:
      sum += pair.distance
    return sum

  def center(self):
    center = 0
    for pair in self.pairs:
      center += np.around(np.multiply(pair.point.components, pair.distance), 5)
    return np.around(np.divide(center, self.sum()), 5)

  def rebalance(self, lower, upper):
    total = self.sum()
    action = None
    baz = 1000/7

    if total <= lower:
      (inc, dec) = foo(min((lower - total) / (baz * 5), 0.01))
      action = inc
    elif total >= upper:
      (inc, dec) = foo(min((total - upper) / (baz * 5), 0.01))
      action = dec
    else:
      return self

    for pair in enumerate(self.pairs):
      if pair[0] <= baz:
        continue
      pair[1].append_action(action)
    return self

  def __str__(self):
    return "Focus " + str(self.focus) + " with " + str(self.pairs)

  def __repr__(self):
      # Spark uses this method when save on text file
      return str(self)
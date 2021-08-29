class Pair:
  def __init__(self, point, centroid, h):
    self.point = point
    self.centroid = centroid
    self.distance = point.distance(centroid, h)

  def normalize_distance(self, total_centroid_distance, k):
    relative_distance = self.distance / total_centroid_distance
    # normalized_distance = (1 - relative_distance) / (k - 1)
    self.distance = relative_distance
    return self
  
  def __str__(self):
      result = "(P: " + str(self.point) + ", C: " + str(self.centroid) + "): " + str(self.distance)
      return result.strip()

  def __repr__(self):
      # Spark uses this method when save on text file
      return str(self)
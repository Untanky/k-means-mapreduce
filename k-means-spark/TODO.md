# Adapt k-means to balanced k-means

1. assignment $y_j = \{1/n, ..., 1/n\}$

1. Change mapping function
   - **is**:
     - `assign_centroids` returns a tuple of centroid and assigned point
   - **should**:
     - return the relative distance to each centroid and the respective point
2. Adapt reduce function
   - **is**:
     - simply adds all points respective to a centroid up
   - **should**
     - add the distance weighted point to the centroid and

$$c_i = \frac{1}{\sum_{j=1}^ny_{ij}} \sum_{j=1}^ny_{ij}x_j$$
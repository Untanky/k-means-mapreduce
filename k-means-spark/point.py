import numpy as np
from numpy import linalg

class Point:
    ## Initialize a new point from a string
    ## Splits components at ','
    ## Floats are rounded to 5 digits
    def __init__(self, line):
        values = line.split(",")
        self.components = np.array([round(float(k), 5) for k in values])
        self.number_of_points = 1

    ## Add new point to the point componentswise
    def sum(self, p):
        self.components = np.add(self.components, p.components)
        self.number_of_points += p.number_of_points
        return self

    ## calculate the distance to the point p
    def distance(self, p, h):
        if (h < 0):
           h = 2
        return round(linalg.norm(self.components - p.components, h), 5)

    ## Recenter point
    def get_average_point(self):
        self.components = np.around(np.divide(self.components, self.number_of_points), 5)
        return self
    
    def __str__(self):
        result = ""
        for component in self.components:
            result += str(component)
            result += " "
        return result.strip()

    def __hash__(self):
        return hash(str(self.components))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __repr__(self):
        # Spark uses this method when save on text file
        result = ""
        for component in self.components:
            result += str(component)
            result += " "
        return result.strip()
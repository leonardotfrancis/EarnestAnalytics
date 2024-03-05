
from enum import Enum

class GroupByType(str, Enum):
    water = 'address_category_water'
    relief = 'address_category_relief'
    flat = 'address_category_flat'
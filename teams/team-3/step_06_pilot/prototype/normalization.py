"""Strict, lossless normalization for scalar data from JSON, pandas or Parquet."""
# Reading copy with comments added. The code is unchanged from the module embedded in the deployed v18 notebook.
#
# Purpose: station feed values can arrive as Python ints, floats such as 3.0,
# numpy types, or booleans. These helpers convert them to plain int and bool
# values, and refuse anything that would silently change meaning (for example
# 3.5 bikes, NaN, or a boolean used as a count).

## Packages
import math  # Check that numbers are finite (not NaN or infinity)
import numbers  # Recognize any real number type, including numpy numbers


## Convert one value to a whole-number count
def integer(value):
    # Refuse booleans first: in Python True == 1, so a flag could pass as a count.
    if isinstance(value, bool) or (type(value).__module__.startswith('numpy') and type(value).__name__ in ('bool', 'bool_')):
        raise ValueError('boolean is not a count')
    # Refuse text, None and other non-numeric values.
    if not isinstance(value, numbers.Real):
        raise ValueError('expected a numeric integer')
    # Refuse NaN, infinity and fractional values such as 3.5 (3.0 is accepted).
    if not math.isfinite(value) or int(value) != value:
        raise ValueError('expected a finite integral value')
    return int(value)  # Return a plain Python int


## Convert one value to a True/False flag
def flag(value):
    # Accept real booleans (Python or numpy) as they are.
    if isinstance(value, bool) or (type(value).__module__.startswith('numpy') and type(value).__name__ in ('bool', 'bool_')):
        return bool(value)
    # Otherwise the value must be a whole number that is exactly 0 or 1.
    v = integer(value)
    if v not in (0, 1):
        raise ValueError('flag must be boolean or 0/1')
    return bool(v)  # Return True for 1 and False for 0


## Extract the six station status fields used by the model
def fields(row):
    # Return the fields in a fixed order; any invalid value stops scoring for this row.
    return (integer(row['num_bikes_available']), integer(row['num_docks_available']),  # Bikes and open docks now
            integer(row['capacity']), flag(row['is_installed']),  # Station size and whether it is installed
            flag(row['is_renting']), flag(row['is_returning']))  # Whether riders can take or return bikes

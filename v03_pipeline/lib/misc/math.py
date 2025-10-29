from collections.abc import Generator
import math

def split_ranges(max_value: int, n: int = 5) -> Generator[tuple[int, int], None, None]:
    if max_value < n:
        yield (0, max_value)
    step = math.ceil(max_value / n)
    start, end = 0, step
    while start < max_value:
        yield (start, min(end, max_value))
        start += step
        end += step

def constrain(
    number: int | float,
    lower_bound: int | float,
    upper_bound: int | float,
) -> int:
    if lower_bound > upper_bound:
        msg = 'Lower bound should be less than or equal to upper bound'
        raise ValueError(msg)
    return max(lower_bound, min(number, upper_bound))

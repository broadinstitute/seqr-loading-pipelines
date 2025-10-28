from collections.abc import Generator


def split_ranges(max_value: int, n: int = 5) -> Generator[tuple]:
    step = max_value // n
    start = 0
    for _ in range(n):
        end = start + step
        yield (start, end - 1)
        start = end
    yield (start, max_value)


def constrain(
    number: int | float,
    lower_bound: int | float,
    upper_bound: int | float,
) -> int:
    if lower_bound > upper_bound:
        msg = 'Lower bound should be less than or equal to upper bound'
        raise ValueError(msg)
    return max(lower_bound, min(number, upper_bound))

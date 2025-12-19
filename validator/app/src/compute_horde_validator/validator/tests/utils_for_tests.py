def lenient_float_factory(rel_tol_: float, abs_tol_: float):
    class LenientFloat:
        rel_tol = rel_tol_
        abs_tol = abs_tol_

        def __init__(self, value: float):
            self._value = value

        def __eq__(self, other):
            if isinstance(other, LenientFloat):
                other = float(other)
            if isinstance(other, int | float):
                return abs(self._value - other) <= max(
                    self.rel_tol * max(abs(self._value), abs(other)), self.abs_tol
                )
            return super().__eq__(other)

        def __str__(self):
            return str(self._value)

        def __repr__(self):
            return repr(self._value)

        def __float__(self):
            return self._value

    return LenientFloat

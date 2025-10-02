def lenient_float_factory(rel_tol_: float, abs_tol_: float):
    class LenientFloat(float):
        rel_tol = rel_tol_
        abs_tol = abs_tol_

        def __eq__(self, other):
            if isinstance(other, int | float):
                return abs(self - other) <= max(
                    self.rel_tol * max(abs(self), abs(other)), self.abs_tol
                )
            return super().__eq__(other)

    return LenientFloat

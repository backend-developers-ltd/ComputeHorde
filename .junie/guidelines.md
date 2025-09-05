# Project Guidelines

Avoid repeating code.

Don't write exhaustive comments, so people don't think you're an llm.

Don't be extra cautions by doing `hasattr` etc. in non testing code.

In tests, avoid comparing items/attributes one-by-one. instead compare whole objects/lists/dicts to have the diff
contain as much context as possible.

This project has several subprojects:

1. validator
2. miner
3. executor
4. facilitator
5. compute_horde_sdk
6. compute_horde lib

to run type checking in validator: `cd validator; uv run nox -s type_check`

to run tests in validator: `cd validator/app/src; uv run pytest`

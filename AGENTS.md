# Project Guidelines

Assume a minimum Python version of 3.11.

Use new-style type declarations - `list[int] | None` instead of `Optional[List[int]]`

Avoid repeating code.

Avoid importing modules inside functions unless you are certain that circular import issues will occur.

Don't write exhaustive comments, so people don't think you're an llm.

Don't be extra cautions by doing `hasattr` etc. in non testing code.

To call an sync function from an async context, you must use `asgiref.sync_to_async` instead of `asyncio.to_thread`.

To call an async function from a sync context, you must use `asgiref.async_to_sync` instead of `asyncio.run`.

In tests, avoid comparing items/attributes one-by-one. instead compare whole objects/lists/dicts to have the diff
contain as much context as possible.

This project has several subprojects:

1. validator
2. miner
3. executor
4. facilitator
5. compute_horde_sdk
6. compute_horde lib

This project uses Astral uv. Use `uv run` to run Python code or Python tools. Do not try to create or activate a venv manually.

to run type checking in validator: `cd validator; uv run nox -s type_check`

to run tests in validator: `cd validator/app/src; uv run pytest`

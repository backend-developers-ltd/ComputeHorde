# Definition

The task is to add support for multiple GPU classes in the synthetic
jobs pipeline and payload for these.

# Terminology

I'm going to consider two ways of adding support for multiple types.
One is - in rough words - to keep existing code mostly as-is, but
parametrize it with GPU class. Only at the very highest level 
a specific GPU class is specified which is then used as the parameter.
Example:
Before:

```
def a():
    b()
    c()
```
    
After:

```
def a():
	for gpu_class in ...:
		b(gpu_class)
		c(gpu_class)
```
		
Let's call this approach "depth-first".

The second way is to add support for multiple GPU class at each level:

```
def a():
	b()
	c()
	
def b():
	for gpu_class in ...:
		...
		
def c():
	for gpu_class in ...:
		...
```

Let's call this approach "breadth-first".

Both approaches can be mixed, by switching from breadth-first to depth-first on an arbitrary abstraction level.

# Observations, discussions

## The payload generation pipeline - high abstraction level

Let's consider the payload generation pipeline consisting of:
- `llm_prompt_generation`
- `llm_prompt_sampling`
- `llm_prompt_answering`

These functions/tasks already rely on being only one GPU class. E.g. in `llm_prompt_generation`:
https://github.com/backend-developers-ltd/ComputeHorde/blob/216af472ca8e120f8d3d9b6df8493d176ec6b44f/validator/app/src/compute_horde_validator/validator/tasks.py#L1171-L1175

The `unprocessed_workloads_count` should be a per-GPU class counter, because even if we have unprocessed workloads for, say A6000, we still may be in need of generating prompts for another GPU class.

OK, so let's consider what happens when we make all these 3 functions parametrized by a GPU class and add an another `@app.task` layer above them.

### Depth-first payload generation pipeline

#### Prompt generation

During prompt generation nothing spectacular happens. The GPU class must be used to correctly short-circuit the task (if unprocessed `SolveWorkload` exist or the number of existing `PromptSeries` is high enough). Finally, it should be used to determine how many prompts a `PromptSeries` should have. Please note, that the actual GPU on which the prompts are generated isn't crucial but since we need an executor
with a specific GPU for prompt answering later on, it seems the least surprising to use the same GPU class for prompt generation too (i.e. `PromptJobGenerator.executor_class` should not be hard-coded.

Also, please note, that we could, in principle, generate prompts suitable for all GPU classes (i.e. generate only series with as many prompts as the GPU with the most VRAM supports), but I discarded that option as we don't seem to be able to elegantly cut the series to the necessary size later on (as we refer the series via `s3_url`) etc.

The above means that the `PromptSeries` must know of a specific GPU class it was constructed for.
Similarly, we should be able to determine whether there are `SolveWorkloads` for specific GPU class.
An additional difficulty is that currently we get the number of prompts in the series via a configuration variable `DYNAMIC_NUMBER_OF_PROMPTS_IN_SERIES`. I'm not sympathetic towards having this as a configuration parameter.
Do we need to configure it? Shouldn't it be a hard-coded trait of a specific GPU class? 
Or maybe we would like to be able to configure specific GPU classes outside the code? FWIW, I think this is outside
the scope of the task at hand. For sake of simplicity, unless I'm conviced otherwise, I'd make this a property of a GPU class.

#### Prompt sampling

Prompt sampling generates tasks (`SolveWorkload` instances) to be solved by the trusted miner.
There is one thing in sampling that doesn't quite agree with the depth-first approach:
https://github.com/backend-developers-ltd/ComputeHorde/blob/216af472ca8e120f8d3d9b6df8493d176ec6b44f/validator/app/src/compute_horde_validator/validator/tasks.py#L1407-L1408

We itereate over a randomly-ordered `PromptSeries`. GPU class is a low-cardinality field so if we filter over it here we might effectively read all the `PromptSeries` for filtering. Is this a problem? Is the number of the series high enough to care? The default `DYNAMIC_MAX_PROMPT_SERIES` is 3500, and each series is, at most, roughly 1kB in size (`s3_url` max length is 1000).
This is ~3.5MB per GPU class. I judge that we can ignore this problem unless proven otherwise (please see also the relevant point in the description of the breadth-first approach)
I don't know that for sure, but since we are able to ensure that `PromptSeries` for specific GPU classes are written to the database consecutively an index might help us to skip unrelated pages even though the cardinality is low. 

#### Prompt answering

Again, there doesn't seem to be anything spectacular happening during prompt answering. We need to take care to iterate over the correct
workloads, and to create jobs for executors with matching GPU class.

#### Unanswered questions
- should we have a single `@app.task` for, say, `llm_prompt_generation`, that iterates over GPU classes, or should we have multiple `@app.task`, one for each GPU class? The latter option is better for handling the unhappy path - each GPU class is processed independently. But there may be other considerations I'm not aware of related to Celery tasks.
- what should happen with the system events? Should the GPU class be included in them?


### Breadth-first approach

Breadth-first approach looks much less appealing for me. It requires more changes in the existing code, and introduces more failure scenarios that we'd need to handle. 
The only case in which I considered it is `llm_prompt_sampling` due to this iteration over `PromptSeries`.
We might introduce something like a notion of a "workload builder" which would keep the state for each GPU class and would be notified of each `PromptSeries` to process. 
On one hand it would be pretty elegant eventually, but on the other I don't find the benefit of iterating over `PromptSeries` only once very appealing. Not appealing enough to make all these code moves and to spend time considering new failures scenarios.

## Synthetic jobs pipeline

Synthethic jobs pipeline looks more nuanced than payload generation. In particular, in `_generate_jobs` a breadth-first approach is used:
https://github.com/backend-developers-ltd/ComputeHorde/blob/216af472ca8e120f8d3d9b6df8493d176ec6b44f/validator/app/src/compute_horde_validator/validator/synthetic_jobs/batch_run.py#L1157-L1161

Here, following the breadth-first approach seems like less work. We could make `get_llm_prompt_samples` return a map from GPU class to `list[PromptSample]`, and use the appropriate one when iterating (again - a map of prompt sample iterators). This time I'm not worried
about the failure scenarios, because the code already is breadth-first, and `get_llm_prompt_samples` contract is simple and clear.

# The proposed design

Let's make it brief:

- introduce `GPUClass` enum;
- model changes:
  - add gpu class to `PromptSeries`
  - add gpu class to `SolveWorkload`
  - I don't think it's needed, but if it turns out differently, add gpu class to `PromptSample`
  - Since I'm coming from nosql world it is natural for me to use denormalized data. I thus reflected on the above choice
    and decided that it is still better to explicitly define gpu class for the above classes rather than introducing
    a relation between them and a separate gpu class entity.
- map `ExecutorClass` to relevant `GPUClass`. I don't know the pythonic way to do it. Should the `ExecutorClass` become something more than `StrEnum`? Or is it fine to have a function `ExecutorClass -> GPUClass`?
- move GPU-specific configurations like `DYNAMIC_NUMBER_OF_PROMPTS_IN_SERIES` or `DYNAMIC_NUMBER_OF_PROMPTS_PER_WORKLOAD` to the `GPUClass`;
again - I don't know the pythonic way to do it. I mean - the `GPUClass` enum could have a `dict` value (at a price of less performant enum, which I don't think we care about), but perhaps this is not how a seasoned python dev would do it.
- change the payload generation pipeline to use the depth-first approach
- use `GPUClass` in the relevant places of the payload generation pipeline
- I think it would be better to have separate celery tasks for each `GPUClass`, but I don't have a strong opinion
- in the synthetic jobs pipeline let's stick to the breadth-first approach, and make `get_llm_prompt_samples` return a map.

Test plan:
- not ready yet

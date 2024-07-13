# Different Job flows

ComputeHorde distinguishes 3 job flows, serving different purposes.

## Synthetic job flow

Used primarily for assessing miner-provided hardware. In this flow, miners are given computational tasks, whose results
are previously known by validators. The nature of these tasks is that they don't require powerful computational hardware
to validate.
![synthetic_job.puml.svg](synthetic_job.puml.svg)


## Organic job flow

Used by ComputeHorde's users to perform any computational tasks. The limitation is that Miners, or rather Executors, in
this flow are black boxes, who receive a job to perform (in the form of a docker image and input volumes) and only
report back w=once they're finished (be it a success or a failure). No network communication for the time of the job
is allowed.

![organic_job.puml.svg](organic_job.puml.svg)]


## Streaming organic job flow

Only possible with whitelisted images, this flow allows for deploying a webservice requiring a lot of computational
resources (like a LLama 70B) and querying it directly. This allows a responsive flow, in which a user waits for a spin
up once and then queries the Job back and forth multiple times for the lifetime of the Job. Accepting and replying to 
requests is the only form of network communication the Job is allowed. Additional data may be fed to the Job. Details
must be, however, passed through the Miner (not directly).

![streaming_organic_job.puml.svg](streaming_organic_job.puml.svg)
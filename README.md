# Goal

Minimal primitives to get an asyncio executor running with Redis Queue to

* Submit tasks
* Poll for results
* Fetch results once completed

with a more loosely coupled contract than offered by the documented TaskIQ interfaces.

## Motivation

Previously, I experimented with [Celery](https://github.com/barmanroys/encrypt-decrypt-asynchronous) to achieve the same
outcome. But the major limitation of Celery is the absence
of [asyncio coroutine](https://docs.python.org/3/library/asyncio-task.html) support. Technically, it is possible to go
around this using `asyncio.run`, but the pattern is ugly and defeats the purpose of coroutine.
While experimenting with alternatives, at first I stumbled upon [ARQ](https://arq-docs.helpmanual.io/#), but also found
out it is in _Sunset_ mode.

Hence, the choice of TaskIQ and this small project serving as a PoC.

### Generic Task Dispatch

A few words on this concept is in order, which is also the reason I felt motivated to write this.
The [TaskIQ landing page](https://taskiq-python.github.io/guide/getting-started.html) gives an easy pattern to invoke a
job asynchronously and wait for the result.
But the important assumption in the pattern (and I cannot emphasise this enough) is

> The client has access to the function, i.e. either defined in the same module or the function is somehow importable

That is just too big of an API surface area, and the contract is unnecessarily large for my taste. Expecting the client
to somehow import or know the full implementation of a _task_ (the basic primitive) is not viable or scalable.
Hence, the attempt to go through the [TaskIQ source code](https://github.com/taskiq-python/taskiq), trying to peel apart
the high level primitives to something a bit lower level, but still intuitive enough.

You can go through the code on your own, (I believe the files are well commented), so I
will keep this README to a minimal.

## Basic Architecture

The core idea is very simple. A Redis server (running on localhost here) serves as the broker and result backend. For a
task
to execute, this is what needs to happen

* Workers wait with `BRPOP` on a redis queue (broker)
* A client places the task (or any number of tasks) on the queue. Part of the task message is a unique task id (a
  string, which should represent a UUID to avoid collision)
* An available worker claims and pops a task (if all workers are busy, the task waits in the queue)
* A worker executes a task it picked up. Upon (successful or unsuccessful) completion, it puts the result (or an error
  message) on the backend (same Redis server in this example, but can be different).

![Architecture](docs/figure.svg)

##### Client-Worker Contract Surface

We already touched upon this once, now is the time to make it more concrete. In the implementation shown in this
project, the contract (shared context) between the client and worker includes

* The broker and result backend URLs
* The queue name (TaskIQ provides
  a [default](https://github.com/taskiq-python/taskiq-redis/blob/main/taskiq_redis/redis_broker.py#L36), but best to use
  one for your project to avoid collision against other
  projects using the same Redis logical DB)
* The result key prefix (same advice as above, using TaskIQ default skips any prefix and runs the risk of unnecessary
  key collision)
* The task _name_ (assuming the worker has different kinds of tasks registered, such as a pipeline dealing with image
  data may be capable of different kinds of filtering, segmentation or image recognition via vision models). A task
  must be identified by its unique name and the client must know the name.
* Function signature of the task (identified by a name), preferably with keyword argument names, data types and return
  type (just like what you should know to invoke a function in python)

That may sound daunting, but probably you still realise the surface area is minimal compared to a full task definition.

Based on this, I have formulated my extremely advanced computation algorithm that can...well, add one to an integer that
you supply, which is defined in `src/worker.py`.
As part of this maddeningly complex computation, I have also introduced a very long delay (as perceived by a processor)
of about five seconds, to see the impact of unfinished tasks.

That is probably all you need in terms of code-walkthrough and design guide. Armed with it, if you look through the
code (two Python files), it should easily make sense.

#### Test the code

I developed on a machine running Ubuntu 26.04, and the dependencies you need

* [Docker compose](https://docs.docker.com/compose/) to fire up a local redis instance. Feel free to use any other redis
  instance if available, and update the `REDIS_URL` in worker and client both.
* [UV Python package manager](https://docs.astral.sh/uv/) (update the commands if you are using other environment
  managers or the base environment of your host)

Here is all you need to do to get an end to end workflow. Fire up the redis by

```shell
docker compose up 
```

Then fire up the worker by

```shell
uv run taskiq worker src.worker:broker --log-level DEBUG --workers 1 --reload # allow hot reload for easy modification 
```

Both of the above commands are blocking, and best to execute on different terminals. You can use `nohup` and `&` but it
will be hard to see the logs which can be very instructive.

Look up the [CLI documentation](https://taskiq-python.github.io/guide/cli.html) if you want to tweak some of the
start-up parameters.

Then, to use the well documented pattern of task invocation and wait (with full knowledge of the task definition)

```shell
time uv run src/worker.py 
```

But that is no fun, as it does not really explore the generic dispatch pattern discussed previously at length. To use
the generic dispatch, run

```shell
time uv run src/client.py 
```

which will show the task lifecycle in its full glory, starting from pushing to the queue, wait for results to appear,
and finally, read the result.

#### Complete Client-Worker Isolation

Those who have a knack of thinking about the infra layer and real production system instead of PoC, would immediately
identify that the worker and client are still defined in the same repository and not isolated enough, for all the noise
about _Generic_ task dispatch.
So here is an idea of how to run them in a totally decoupled manner

* Run the Redis server as its own Kubernetes service (backed by a deployment having one replica)
* Run workers as their own deployment (no service necessary, as the workers need not be discoverable, they send their
  heartbeats to the Redis). Number of workers depends on the workload and you can use horizontal pod autoscaler
* The client is just a K8s job, or it can be a web client (Uvicorn service) which gets the tasks from its own downstream
  client to put on the Redis queue

I did not get the bandwidth to write out the Dockerfiles and Kubernetes manifests for the isolation, but if any of you
feel like getting your hands dirty, feel free to branch out and send a PR. It will be a highly instructive exercise
working the right muscle groups, and prove you have really internalised the
concepts.

If the idea of K8s sounds daunting, you can achieve the same with a more detailed docker compose stack as well, and
again, you are welcome to try.

#### Peek Under the Hood

It is also quite instructive to look at the logs being flashed (both worker process and the redis server) to get a
better appreciation of the transport layer protocol. Based on my limited experimentation, here are some stuff I noticed
by looking at the log and playing around with `redis-cli` to inspect what's being populated.

The queue name, unsurprisingly, is literally the name fo the queue where the client puts the task. The element being
pushed on the queue has the binary representation of the task name, arguments, task id, and some other metadata that can
easily make sense with a bit of inspection.

The key prefix for the result backend, as the name implies, is literally the prefix of the result key. An example key (
from this project) is
`sample_result:6c034898-46c5-4c09-8736-bb71337b6133` where the suffix (after colon) is the task id. The content of this
key represents the result and error message (if any).

So if two projects using TaskIQ are forced to share a redis logical DB, now you know how to prevent a disaster.

Finally, this library, unlike Celery, seems to lack the concept of _task status_ and an explicit polling mechanism. At a
low level, absence of the
key (\<prefix\>:\<task id\>) itself signals the task has not reached a terminal state (which has the same interpretation
as if the task never existed).
This surfaces to the application layer via `ResultIsMissingError` (look at `src/client.py`). Keep this in mind when you
check for results.

#### Best Practices

The Redis service (especially if running on Kubernetes) is meant to be a cattle, not pet. So you should assume the pod
can die any moment and spun up again by Kubernetes.

Hence, do not depend on it to store the results on a long term basis. Rather, if the task has results, then let the
worker put the results in some more durable persistence media (database, data warehouse or object/file storage).

In addition, if you take the above route to store results, also make the tasks themselves idempotent following good data
engineering practice. That means if the same task runs more than once with the same input, the persistence layer must
not end up in an inconsistent stage.

#### Closing Words

I was looking for a similar library in Rust. Based on a bit of reading it appears this is lacking so far, but the
Rust community also encourages a more DIY culture, which is daunting but fun at some level.
I believe based on the understanding from this project (especially the transport layer protocol and serialisation), one
should be able
to wire together a basic async executor using Redis and Rust, although a lot of unknowns remain.
This is certainly something I want to try as soon as I get some bandwidth. In case you want to collaborate or have any
idea, my
[inbox](mailto:swagatopablo@aol.com) is always open.







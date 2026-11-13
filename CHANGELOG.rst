=========
Changelog
=========


1.10.0 (2026-11-12)
===================

- Drop support python 3.10, add python 3.15
- CRON: POSIX semantics for range steps in all fields (breaking change for schedules with day/month)
- Agent hooks: run ``pre_stop`` hooks before transport shutdown ``post_stop`` after


1.9.1 (2026-11-11)
==================

- Fix exit code of failed agent in launcher (SystemExit(1) instead of 0)
- Fix swallowing of external cancellation in RPC (call/waiter) on graceful shutdown
- RPC timeout now closes the response queue instead of canceling the caller task
- AMQP: log all handler errors with traceback (including TypeError)
- AMQP: fix rebind attempts off-by-one (3 attempts as documented)


1.9.0 (2026-07-29)
==================

- New ``configure()`` for loading signals and queues
- ``Signal`` and ``Queue`` class attribute access
- ``DoubleLoadError`` for duplicate loading
- Docs and examples update


1.8.3 (2026-06-18)
==================

- Broker graceful shutdown with task tracking
- Broker fixes and updates
- Signal and queue hash over name

1.8.2 (2026-06-11)
==================

- Receiver/bus graceful cancellation
- Fix exception handling


1.8.1 (2026-06-06)
==================

- Improves os process control, fix graceful exit
- CRON tasks now has OR logic for calendar and week days
- Periodic/cron graceful cancellation


1.8.0 (2026-02-26)
==================

- The broker now supports the use of exchanges and topic-based routing
- Drop support python 3.9, add python 3.13, 3.14


1.7.3 (2024-03-12)
================

- RPC bugfix


1.7.2 (2024-03-05)
================

- Typing fixes


1.7 (2023-10-01)
================

- Change AMQP driver to aiormq
- Change Redis driver to redis (redis.asyncio)
- Drop setup.(py|cfg), add pyproject.toml
- Drop support python 3.8, add python 3.11, 3.12
- Redesigned linking system
- Typing fixes


1.6.1 (2022-05-29)
================

- Fix AMQP consumers rebinding


1.6 (2022-02-20)
================

- Optional runtime type checking for signals (send, call, receiver)
- Optional DTO for consumers
- aioamqp 0.15


1.5 (2022-01-22)
================

- Improved RPC via bus (call)
- Typing


1.4 (2021-08-22)
================

- Update for aioredis 2.0
- Enable support python 3.10


1.3 (2021-05-10)
================

- Launcher updated
- Removed croniter dependency


1.2 (2021-02-20)
================

- Update for python 3.9
- Update aiokafka to 0.7


1.1 (2020-09-15)
================

- Update documentaion
- Fixed AMQP driver
- Fixed typing
- Fixed launcher


1.0 (2020-07-04)
================

The project is almost completely rewritten


0.9 (2019-12-04)
================

- Update for python 3.9


0.8 (2019-09-09)
================

- Bugfixes

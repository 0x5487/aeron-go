# Changelog

This is the trading-poc fork of [lirm/aeron-go](https://github.com/lirm/aeron-go), branch `trading-poc`. It lists the
changes the fork carries on top of upstream (last upstream merge: 8b05ad1, pull request #110). Every change records why
it was made and where it was found (trading-poc experiments under `docs/labs/aeron/`, tickets under `docs/tickets/`).

## Unreleased

### Fixed

- **driver**: unknown driver events are logged once at Warn and skipped instead of `Fatalf` (7371f05). Drivers from
  Aeron 1.49.0 on broadcast `ON_NEXT_AVAILABLE_SESSION_ID` (0x0F0D) to every client; the Java client ignores events it
  does not know, and the fork killed the process. Found in trading-poc experiment 01.
- **conductor**: `closeAllResources` reads and clears publications, subscriptions and counters under `adminLock`
  (1f1e528). Closing a client while the conductor was processing was a data race. Found in trading-poc experiment 07.
- **publication**: `Offer`, `Offer2` and `TryClaim` over the maximum message size return the new code
  `MessageTooLong` (-6) instead of panicking (7a08d71). A cluster leader replying with an oversized message died, and
  the retried command killed the next leader too. Adds `Publication.MaxMessageLength` and `NewTestingPublication`;
  `Cluster.Offer` returns the code to the caller. Found in trading-poc experiment 04.
- **archive**: `RecordingSignalEvent` reaches the listener on every poll path; template 106 (Ping) is skipped at debug
  level instead of printing "Unexpected message type" (cb253bc). `Control.Poll` decoded the signal and dropped it, and
  `PollForErrorResponse` called the listener twice for its own session and once for foreign ones. Found in trading-poc
  experiment 05.
- **archive tests**: the mock-based unit tests run when no media driver is available (bac340b); `TestMain` returned
  before `m.Run()` and every test was skipped silently. The rotted control tests are fixed.

### Added

- **archive**: `DefaultPollInterval` and `Options.SetPollInterval` (5ed7628). The 50 ms sleep between control-response
  polls was a literal and the latency floor of every synchronous Archive call; the default stays 50 ms.
- **aeron**: `Context.InterServiceTimeout` reaches the client conductor (28c3dd0); unset, the driver's client liveness
  timeout still applies.

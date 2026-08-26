# TAGalysis reliability capacity model

Measured from production on 2026-08-26 after the five-second polling repair:

- normal ten-minute cycle upper bound: 394 governed database statements;
- additional hourly work: 770 statements;
- idle queue claim: at most one statement every 120 seconds after backoff.

The conservative daily projection is
`144 * 394 + 24 * 770 + 720 = 75,936` statements. The former 10,000/day
number covered scheduler overhead only and was not a valid whole-process
budget. TAGalysis therefore uses a 100,000/day and 3,100,000/31-day telemetry
capacity, leaving about 24.1% daily headroom. The counter remains telemetry;
it never interrupts database reads or writes.

Render health probes are side-effect-free and do not open a database session.
Neon platform-monitoring SQL is excluded from this application projection.
The model must be revised when cadence or enabled job families change.

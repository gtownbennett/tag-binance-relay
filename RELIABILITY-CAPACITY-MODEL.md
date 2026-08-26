# TAGneXt reliability capacity model

Measured from the isolated production process on 2026-08-26 after the
five-second polling repair:

- normal ten-minute cycle upper bound: 904 governed database statements;
- additional hourly work: 1,563 statements;
- idle queue claim: at most one statement every 120 seconds after backoff.

The conservative daily projection is
`144 * 904 + 24 * 1,563 + 720 = 168,408` statements. The former 10,000/day
number covered scheduler overhead only and was not a valid whole-process
budget. TAGneXt therefore uses a 200,000/day and 6,200,000/31-day telemetry
capacity, leaving about 15.8% daily headroom. The counter remains telemetry;
it never interrupts database reads or writes.

Render health probes are side-effect-free and do not open a database session.
Neon platform-monitoring SQL is excluded from this application projection.
The model must be revised when cadence, enabled job families, or provider
coverage changes.

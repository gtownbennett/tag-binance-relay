# RC4 functional release and promotion policy

RC4 functional completion and challenger promotion are separate gates.

RC4 may be marked `RC4_PASSED` only when the tested source is committed, the
stable-signed `com.eric.tagnext` APK passes installed-device acceptance, the
existing challenger deployment passes health and authenticated populated-screen
acceptance, NodeReal and Coinalyze are both fresh/read-only/exact-TAG at runtime,
and TAGalysis access is either proven transaction-read-only or disabled.

The TAGalysis comparison does not block functional release. It does block every
winner or learning claim. TAGalysis remains champion while there are fewer than
30 clean preregistered exact pairs per horizon. Pairs require identical asset
identity, issue time, horizon, deadline, outcome definition, and outcome ID.
Backdating, timestamp tolerance, copied champion contents, and fabricated pairs
are prohibited. Automatic promotion is always off.

The currently disabled `tagalysis_history_importer` is a safe release state: it
cannot write and cannot log in. It means live champion synchronization is paused;
the checksum-verified existing champion export remains the comparison baseline.
Enabling the importer later requires a dedicated credential, `LOGIN`, unlimited
validity, `default_transaction_read_only=on`, the existing three-table `SELECT`
allow-list, zero table writes, and a role-self proof including rejected writes
with SQLSTATE 25006.

NodeReal and Coinalyze are runtime shadow evidence. Their data is owner-visible,
freshness-gated, read-only, and excluded from forecast weights. Missing provider
values stay unavailable. Coinalyze liquidation rows are not relabeled as a
liquidation heatmap. Future forecast influence requires a separate preregistered
out-of-sample promotion review.

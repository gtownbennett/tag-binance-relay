from __future__ import annotations

import os


# Unit tests must opt into the only mode where a local SQLite challenger is
# allowed. Production/preview imports require TAGNEXT_DATABASE_URL.
os.environ.setdefault("SYSTEM_ID", "tagnext")
os.environ.setdefault("FORECAST_PRODUCER", "tagnext")
os.environ.setdefault("TAGNEXT_RUNTIME_MODE", "unit_test")

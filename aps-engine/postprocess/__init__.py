"""Post-processing utilities (audit report adapter/runner).

Important:
- Keep this package *side-effect free*.
- Do **not** import heavy/optional submodules at import time.

This avoids crashing the APS CLI when optional reporting dependencies
(or report generator code) are temporarily broken.
"""

# Intentionally empty. Import what you need explicitly, e.g.:
#   from bohae_aps_v20.postprocess.audit_runner import safe_generate_rich_report

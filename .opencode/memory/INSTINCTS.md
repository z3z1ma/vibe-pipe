# INSTINCTS

This is the *fast index* of the current instinct set.
The source of truth is `.opencode/memory/instincts.json`.

<!-- BEGIN:compound:instincts-md -->
## Active instincts (top confidence)

- **optional-dependency-import-pattern** (90%)
  - Trigger: Need to use external library that may not be installed
  - Action: 1. Wrap import in try/except block 2. Add type: ignore[import-untyped] comment for mypy 3. Set flag variable to track availability 4. Provide fallback behavior when unavailable 5. Document degradation…
- **datetime-none-coalescing** (85%)
  - Trigger: Performing datetime arithmetic with potentially None fields
  - Action: Always use: (field or datetime.utcnow()) to ensure arithmetic operations have datetime, not Optional[datetime]
- **formatter-type-separation** (75%)
  - Trigger: MyPy complains about incompatible formatter assignments
  - Action: Create distinct variables (json_formatter, colored_formatter, simple_formatter) instead of reusing single 'formatter' variable to avoid type checker confusion

## Notes

- Instincts are the *pre-skill* layer: small, repeatable heuristics.
- When an instinct proves useful across sessions, promote it into a Skill.
<!-- END:compound:instincts-md -->

---
"id": "vp-ba2a"
"status": "closed"
"deps":
- "vp-d359"
"links": []
"created": "2026-01-27T14:41:02Z"
"type": "task"
"priority": 2
"assignee": "z3z1ma"
"tags": []
"external": {}
---
Implemented @expect decorator with full test coverage:

- Added Expectation type to types.py with validation logic
- Implemented @expect decorator supporting both @expect and @expect(...) patterns
- Functions can return bool or ValidationResult for flexibility
- Supports parameters: name, severity (error/warning/info), description, metadata, config
- Auto-uses function docstring as description
- Added 20 comprehensive tests covering all usage patterns
- All 31 decorator tests passing
- Mypy strict mode passes for decorators module (source files)
- 100% test coverage for decorators module
- Updated exports in __init__.py
- Updated pyproject.toml to exclude tests from strict mypy checking

Commit: 5b285e8
Branch: murmur/vp-ba2a
Ready for manager review

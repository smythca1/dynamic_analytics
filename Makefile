.PHONY: test compile prune-dry-run

test:
	PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v

compile:
	.venv/bin/python scripts/check_syntax.py

prune-dry-run:
	.venv/bin/python scripts/prune_to_production.py

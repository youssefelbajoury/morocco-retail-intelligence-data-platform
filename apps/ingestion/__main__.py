"""Run with: python -m apps.ingestion ingest --source sales --path <file>"""

from mri.ingestion.cli import main

if __name__ == "__main__":
    raise SystemExit(main())

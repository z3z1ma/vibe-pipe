"""
Tests for the simple pipeline example.

Tests verify:
- Pipeline runs successfully
- All steps produce correct data
- Summary statistics are accurate
"""

from __future__ import annotations

from vibe_piper import (
    DefaultExecutor,
    PipelineBuilder,
    PipelineContext,
    UpstreamData,
)


def test_pipeline_runs_successfully() -> None:
    """Test that pipeline builds and executes successfully."""
    builder = PipelineBuilder(
        name="simple_text_pipeline",
        description="Simple text processing pipeline",
    )

    # Asset 1: Source - raw text data
    def raw_text_source(*args):  # noqa: ARG001
        """Source asset with raw text data."""
        return [
            "  hello world  ",
            "  VIBE PIPER  ",
            "  data pipeline  ",
            "  simple example  ",
            "  minimal code  ",
        ]

    builder.asset(
        name="raw_text",
        fn=raw_text_source,
        depends_on=[],
        description="Raw input text with extra whitespace and mixed case",
    )

    # Asset 2: Clean - normalize whitespace and case
    def clean_text(upstream_data, context):  # noqa: ARG001
        """Clean text by stripping whitespace and converting to lowercase."""
        raw_text = upstream_data["raw_text"]
        return [text.strip().lower() for text in raw_text]

    builder.asset(
        name="clean_text",
        fn=clean_text,
        depends_on=["raw_text"],
        description="Normalize whitespace and case",
    )

    # Asset 3: Transform to structured records
    def transform_to_dicts(upstream_data, context):  # noqa: ARG001
        """Transform clean text to structured records with metadata."""
        clean_text = upstream_data["clean_text"]
        return [
            {
                "id": idx,
                "text": text,
                "word_count": len(text.split()),
                "first_char": text[0] if text else "",
                "last_char": text[-1] if text else "",
                "is_palindrome": text == text[::-1] if text else False,
            }
            for idx, text in enumerate(clean_text, start=1)
        ]

    builder.asset(
        name="structured_records",
        fn=transform_to_dicts,
        depends_on=["clean_text"],
        description="Transform to structured records with metadata",
    )

    # Asset 4: Filter - keep only records with 2+ words
    def filter_records(upstream_data, context):  # noqa: ARG001
        """Filter to keep only records with 2 or more words."""
        records = upstream_data["structured_records"]
        return [r for r in records if r["word_count"] >= 2]

    builder.asset(
        name="filtered_records",
        fn=filter_records,
        depends_on=["structured_records"],
        description="Filter to keep records with 2+ words",
    )

    # Asset 5: Summarize - compute statistics
    def summarize(upstream_data, context):  # noqa: ARG001
        """Compute summary statistics."""
        records = upstream_data["filtered_records"]
        total_records = len(records)
        if total_records == 0:
            return {"total": 0, "avg_words": 0, "palindromes": 0}

        total_words = sum(r["word_count"] for r in records)
        palindrome_count = sum(1 for r in records if r["is_palindrome"])

        return {
            "total_records": total_records,
            "total_words": total_words,
            "avg_words": total_words / total_records,
            "palindrome_count": palindrome_count,
            "unique_chars": len(set(r["first_char"] for r in records)),
        }

    builder.asset(
        name="summary",
        fn=summarize,
        depends_on=["filtered_records"],
        description="Compute summary statistics",
    )

    # Create context and executor
    context = PipelineContext(
        pipeline_id="simple_text_pipeline",
        run_id="test-run-001",
        config={"log_level": "INFO"},
    )

    executor = DefaultExecutor()

    # Build and execute pipeline
    graph = builder.build()

    # Execute pipeline
    results = {}
    for asset in graph.assets:
        upstream_data = UpstreamData(raw=results)
        result = executor.execute(asset, context, upstream_data=upstream_data)
        assert result.success, f"Asset {asset.name} failed: {result.error}"
        results[asset.name] = result.data

    # Verify all assets produced data
    assert "raw_text" in results
    assert "clean_text" in results
    assert "structured_records" in results
    assert "filtered_records" in results
    assert "summary" in results

    # Verify intermediate results
    raw_text = results["raw_text"]
    assert len(raw_text) == 5
    assert "  hello world  " in raw_text
    assert "  VIBE PIPER  " in raw_text
    assert "  data pipeline  " in raw_text
    assert "  simple example  " in raw_text
    assert "  minimal code  " in raw_text

    clean_text = results["clean_text"]
    assert isinstance(clean_text, list), f"clean_text should be a list, got {type(clean_text)}"
    assert len(clean_text) == 5
    assert "hello world" in clean_text
    assert "vibe piper" in clean_text
    assert "data pipeline" in clean_text
    assert "simple example" in clean_text
    assert "minimal code" in clean_text

    structured_records = results["structured_records"]
    assert len(structured_records) == 5
    assert structured_records[0]["id"] == 1
    assert structured_records[0]["text"] == "hello world"
    assert structured_records[0]["word_count"] == 2
    assert structured_records[0]["first_char"] == "h"
    assert structured_records[0]["last_char"] == "d"
    assert structured_records[0]["is_palindrome"] is False

    filtered_records = results["filtered_records"]
    # All input records have 2 words, so all should pass filter
    assert len(filtered_records) == 5

    # Verify summary statistics
    summary = results["summary"]
    assert summary["total_records"] == 5
    assert summary["total_words"] == 10
    assert summary["avg_words"] == 2.0
    assert summary["palindrome_count"] == 0
    assert summary["unique_chars"] == 5

"""
Tests for the simple pipeline example.

Tests cover:
- Pipeline builds and executes successfully
- Output matches expected deterministic results
- All steps produce correct data
"""

from __future__ import annotations

from vibe_piper import (
    DefaultExecutor,
    PipelineBuilder,
    PipelineContext,
    UpstreamData,
)


def build_pipeline() -> tuple[PipelineBuilder, PipelineContext, DefaultExecutor]:
    """
    Build the simple pipeline for testing.

    Returns:
        Tuple of (builder, context, executor)
    """
    builder = PipelineBuilder(
        name="simple_text_pipeline",
        description="Simple text processing pipeline",
    )

    # Asset 1: Source - raw text data
    def raw_text_source(*args):  # noqa: ARG001
        """Source asset with raw text data - handles any argument signature."""
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

    return builder, context, executor


def test_pipeline_builds_and_executes_successfully() -> None:
    """Test that pipeline builds and executes successfully."""
    builder, context, executor = build_pipeline()

    graph = builder.build()

    # Execute pipeline
    results = {}
    for asset in graph.assets:
        upstream_data = UpstreamData(raw=results)
        result = executor.execute(asset, context, upstream_data=upstream_data)
        assert result.success, f"Asset {asset.name} failed: {result.error}"
        results[asset.name] = result.data

    # All assets should have produced data
    assert "raw_text" in results
    assert "clean_text" in results
    assert "structured_records" in results
    assert "filtered_records" in results
    assert "summary" in results


def test_raw_text_source_data() -> None:
    """Test that raw text source produces expected data."""
    builder, context, executor = build_pipeline()
    graph = builder.build()

    results = {}
    upstream_data = UpstreamData(raw=results)
    result = executor.execute(graph.assets[0], context, upstream_data=upstream_data)
    assert result.success

    raw_text = result.data
    assert len(raw_text) == 5
    assert "  hello world  " in raw_text
    assert "  VIBE PIPER  " in raw_text
    assert "  data pipeline  " in raw_text
    assert "  simple example  " in raw_text
    assert "  minimal code  " in raw_text


def test_clean_text_transform() -> None:
    """Test that clean text removes whitespace and lowercases."""
    builder, context, executor = build_pipeline()
    graph = builder.build()

    results = {}
    result1 = executor.execute(graph.assets[0], context, upstream_data=UpstreamData(raw=results))
    results["raw_text"] = result1.data

    result2 = executor.execute(graph.assets[1], context, upstream_data=UpstreamData(raw=results))
    assert result2.success

    clean_text = result2.data
    assert len(clean_text) == 5
    assert "hello world" in clean_text
    assert "vibe piper" in clean_text
    assert "data pipeline" in clean_text
    assert "simple example" in clean_text
    assert "minimal code" in clean_text


def test_structured_records_transform() -> None:
    """Test that structured records have correct metadata."""
    builder, context, executor = build_pipeline()
    graph = builder.build()

    results = {}
    result1 = executor.execute(graph.assets[0], context, upstream_data=UpstreamData(raw=results))
    results["raw_text"] = result1.data

    result2 = executor.execute(graph.assets[1], context, upstream_data=UpstreamData(raw=results))
    results["clean_text"] = result2.data

    result3 = executor.execute(graph.assets[2], context, upstream_data=UpstreamData(raw=results))
    assert result3.success
    records = result3.data

    assert len(records) == 5
    assert records[0]["id"] == 1
    assert records[0]["text"] == "hello world"
    assert records[0]["word_count"] == 2
    assert records[0]["first_char"] == "h"
    assert records[0]["last_char"] == "d"
    assert records[0]["is_palindrome"] is False

    assert records[1]["id"] == 2
    assert records[1]["text"] == "vibe piper"
    assert records[1]["word_count"] == 2
    assert records[1]["first_char"] == "v"
    assert records[1]["last_char"] == "r"
    assert records[1]["is_palindrome"] is False

    assert records[2]["id"] == 3
    assert records[2]["text"] == "data pipeline"
    assert records[2]["word_count"] == 2
    assert records[2]["first_char"] == "d"
    assert records[2]["last_char"] == "e"
    assert records[2]["is_palindrome"] is False

    assert records[3]["id"] == 4
    assert records[3]["text"] == "simple example"
    assert records[3]["word_count"] == 2
    assert records[3]["first_char"] == "s"
    assert records[3]["last_char"] == "e"
    assert records[3]["is_palindrome"] is False

    assert records[4]["id"] == 5
    assert records[4]["text"] == "minimal code"
    assert records[4]["word_count"] == 2
    assert records[4]["first_char"] == "m"
    assert records[4]["last_char"] == "e"
    assert records[4]["is_palindrome"] is False


def test_filtering_works_correctly() -> None:
    """Test that filtering keeps only records with 2+ words."""
    builder, context, executor = build_pipeline()
    graph = builder.build()

    # Execute all assets
    results = {}
    for asset in graph.assets:
        upstream_data = UpstreamData(raw=results)
        result = executor.execute(asset, context, upstream_data=upstream_data)
        assert result.success
        results[asset.name] = result.data

    filtered = results["filtered_records"]
    # All input records have 2 words, so all should pass filter
    assert len(filtered) == 5


def test_summary_statistics_are_accurate() -> None:
    """Test that summary statistics are correctly computed."""
    builder, context, executor = build_pipeline()
    graph = builder.build()

    # Execute all assets
    results = {}
    for asset in graph.assets:
        upstream_data = UpstreamData(raw=results)
        result = executor.execute(asset, context, upstream_data=upstream_data)
        assert result.success
        results[asset.name] = result.data

    summary = results["summary"]
    assert summary["total_records"] == 5
    assert summary["total_words"] == 10
    assert summary["avg_words"] == 2.0
    assert summary["palindrome_count"] == 0
    assert summary["unique_chars"] == 5


def test_deterministic_output() -> None:
    """Test that output is deterministic (no random or time-based fields)."""
    builder, context, executor = build_pipeline()
    graph = builder.build()

    # Execute pipeline once
    results1 = {}
    for asset in graph.assets:
        upstream_data = UpstreamData(raw=results1)
        result1 = executor.execute(asset, context, upstream_data=upstream_data)
        assert result1.success
        results1[asset.name] = result1.data

    # Execute pipeline second time
    results2 = {}
    for asset in graph.assets:
        upstream_data = UpstreamData(raw=results2)
        result2 = executor.execute(asset, context, upstream_data=upstream_data)
        assert result2.success
        results2[asset.name] = result2.data

    # Output should be identical
    assert results1 == results2

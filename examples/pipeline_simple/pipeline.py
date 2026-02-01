"""
Example: Simple Pipeline

This example demonstrates a minimal, fast pipeline that shows:
- Pipeline builder pattern
- Operator composition
- In-memory data processing
- Text processing flow: clean, transform, filter, and summarize
"""

from vibe_piper import (
    DefaultExecutor,
    PipelineBuilder,
    PipelineContext,
    UpstreamData,
)


def main() -> None:
    """
    Run the simple pipeline example.

    Pipeline flow:
    1. Source: Raw text data
    2. Clean: Normalize whitespace and case
    3. Transform: Convert to structured records
    4. Filter: Keep only valid records
    5. Summarize: Compute statistics
    """
    print("=" * 70)
    print("Simple Pipeline Example")
    print("=" * 70)
    print()

    # Step 1: Build the pipeline
    print("Building pipeline...")
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

    # Build the pipeline
    graph = builder.build()
    print(f"✓ Built pipeline with {len(graph.assets)} assets")
    print()

    # Step 2: Execute the pipeline
    print("Executing pipeline...")
    print()

    executor = DefaultExecutor()
    context = PipelineContext(
        pipeline_id="simple_text_pipeline",
        run_id="demo-run-001",
        config={"log_level": "INFO"},
    )

    # Execute the pipeline
    results = {}
    for asset in graph.assets:
        print(f"  Executing: {asset.name}...")
        # Wrap results in UpstreamData before passing to executor
        upstream_data = UpstreamData(raw=results)
        result = executor.execute(asset, context, upstream_data=upstream_data)
        results[asset.name] = result.data if result.success else None

        if not result.success:
            print(f"    ✗ Failed: {result.error}")
            return

    # Step 3: Display results
    print()
    print("=" * 70)
    print("Results")
    print("=" * 70)
    print()

    # Show intermediate results
    print("Step 1: Raw Text")
    print("-" * 70)
    for text in results["raw_text"]:
        print(f"  '{text}'")
    print()

    print("Step 2: Clean Text")
    print("-" * 70)
    for text in results["clean_text"]:
        print(f"  '{text}'")
    print()

    print("Step 3: Structured Records")
    print("-" * 70)
    for record in results["structured_records"]:
        print(f"  {record}")
    print()

    print("Step 4: Filtered Records (2+ words)")
    print("-" * 70)
    for record in results["filtered_records"]:
        print(f"  {record}")
    print()

    # Show summary
    print("Step 5: Summary")
    print("-" * 70)
    summary = results["summary"]
    print(f"  Total records: {summary['total_records']}")
    print(f"  Total words: {summary['total_words']}")
    print(f"  Average words per record: {summary['avg_words']:.2f}")
    print(f"  Palindrome count: {summary['palindrome_count']}")
    print(f"  Unique first characters: {summary['unique_chars']}")
    print()

    print("=" * 70)
    print("✓ Pipeline executed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    main()

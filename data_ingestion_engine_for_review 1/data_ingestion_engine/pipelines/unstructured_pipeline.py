"""
pipelines/unstructured_pipeline.py
-----------------------------------
Simplified unstructured ingestion pipeline:
    S3 source → GenericUnstructuredReader → ECSHttpParser → NoiseFilter → S3JsonWriter

ARCHITECTURE:
  - GenericUnstructuredReader lists files from S3 and yields file metadata + bytes
  - ECSHttpParser sends all formats to ECS Docling service for parsing
  - NoiseFilter removes browser artifacts, URLs, and unwanted content (PRIORITY 4)
  - No format-specific readers needed (ECS handles 20+ formats)

SCALABILITY: Adding a new format requires ZERO code changes.
  ECS Docling service automatically handles all document formats via Docling library.

MODULARITY: Each component is imported independently.
  GenericUnstructuredReader — handles S3 listing and download
  ECSHttpParser            — delegates parsing to ECS Docling service
  NoiseFilter              — removes browser artifacts and noise (PRIORITY 4)
  S3JsonWriter             — writes ExtractionRecords to target S3

LOOSE COUPLING: Components communicate only through the
  ExtractionRecord dict contract. No class inherits from another
  across pipeline boundaries.
"""

import logging
import re
from typing import Dict, Any, List

log = logging.getLogger("unstructured_pipeline")


# ============================================================================
# PII masking — applied inline, before noise filtering
# ============================================================================

_EMAIL_PATTERN = re.compile(r'[\w.+-]+@[\w-]+\.[\w.-]+')


def mask_email_addresses(text: str) -> str:
    """Redact an embedded email address rather than deleting the whole block
    that carries one.

    A sponsor/medical-monitor contact email in a clinical protocol is real,
    meaningful content — not noise — so it must be masked in place (matching
    the "redact" PII strategy already used elsewhere in this pipeline), not
    silently dropped. Masking here also has a side effect that fixes the
    reported bug directly: with the "@domain.tld" portion replaced, the
    domain-URL noise pattern in filter_noise_blocks() below no longer has
    anything email-shaped left to match, so it no longer flags the block as
    noise and deletes it.

    Uses "***EMAIL REDACTED***" (matching this codebase's existing
    "***REDACTED***" PII convention in silver_layer.py) rather than a
    bracketed placeholder — clean_inline_artifacts() below strips a "["
    or "]" whenever it's adjacent to whitespace as a stray-bracket OCR
    artifact, which silently ate the opening bracket of a literal
    "[EMAIL REDACTED]" placeholder (e.g. "Email: [EMAIL REDACTED]" became
    "Email: EMAIL REDACTED]"). Asterisks aren't in any artifact pattern.
    """
    return _EMAIL_PATTERN.sub("***EMAIL REDACTED***", text)


# ============================================================================
# Table-of-Contents reclassification
# ============================================================================

_TOC_DOT_LEADER_PATTERN = re.compile(r'\.{4,}')


def _looks_like_toc_table(block: Dict[str, Any]) -> bool:
    """A Table of Contents / List of Tables / List of Figures page renders
    structurally as a table (columns of section title, dot-leader, page
    number), but it's a navigation aid, not data — Docling's table-structure
    model has no way to distinguish the two, since both are just grids of
    text. Detect it by the one thing a real data table never has: the
    "....." dot-leader used to visually connect a title to its page number.
    """
    if block.get("type") != "table":
        return False
    rows = block.get("table_rows")
    if rows:
        dot_leader_rows = sum(
            1 for row in rows if any(_TOC_DOT_LEADER_PATTERN.search(cell) for cell in row)
        )
        return dot_leader_rows >= max(1, len(rows) // 3)
    return bool(_TOC_DOT_LEADER_PATTERN.search(block.get("text", "")))


def reclassify_toc_tables(blocks: List[Dict[str, Any]],
                           removal_reasons: Dict[str, int] = None) -> List[Dict[str, Any]]:
    """Retag ToC-shaped tables as layout_type="toc" instead of type="table",
    so a consumer reading type=="table" for structured data doesn't get
    navigation pagination mixed in with real tables (Table 1, the Schedule
    of Activities, etc.). The dot leaders are stripped from the display
    text since they only existed to align content on a printed page.
    """
    reclassified = 0
    for block in blocks:
        if not _looks_like_toc_table(block):
            continue

        rows = block.pop("table_rows", None)
        if rows:
            text = "\n".join(
                "  ".join(_TOC_DOT_LEADER_PATTERN.sub(" ", cell).strip() for cell in row if cell.strip())
                for row in rows
            )
        else:
            text = _TOC_DOT_LEADER_PATTERN.sub(" ", block.get("text", ""))

        block["type"] = "text"
        block["layout_type"] = "toc"
        block["text"] = re.sub(r'\s{2,}', ' ', text).strip()
        reclassified += 1

    if reclassified:
        log.info(f"TOC FILTER: Reclassified {reclassified} table block(s) as layout_type=toc")
        if removal_reasons is not None:
            removal_reasons["reclassified_toc_table"] = reclassified

    return blocks


# ============================================================================
# PRIORITY 4: Noise Filtering Functions
# ============================================================================

def filter_noise_blocks(blocks: List[Dict[str, Any]], removal_reasons: Dict[str, int] = None) -> List[Dict[str, Any]]:
    """
    PRIORITY 4: Filter out browser artifacts, URLs, and noise from extracted blocks.

    Removes:
    - URLs (http://, https://, admin.domain.com)
    - Standalone dates (MM/DD/YYYY, YYYY-MM-DD)
    - Page numbers (Page X of Y, X/Y)
    - Navigation artifacts (breadcrumbs, menu items)
    - Browser chrome (bookmarks, address bar text)
    - Very short blocks (< 10 chars, likely noise)
    - Stray symbols and artifacts (§, ©, }, {, etc.)

    Also cleans inline artifacts from text blocks.

    Args:
        blocks: List of content blocks from parser
        removal_reasons: optional dict to accumulate {reason: count} into, so
            callers can see WHY blocks were removed — not just how many —
            to verify the raw output contained what was expected before this
            filter ran, without a separate debug artifact.

    Returns:
        Filtered list of blocks with cleaned text
    """
    if not blocks:
        return blocks

    filtered_blocks = []

    def _record(reason: str) -> None:
        if removal_reasons is not None:
            removal_reasons[reason] = removal_reasons.get(reason, 0) + 1

    # PRIORITY 4: Define noise patterns (regex)
    noise_patterns = [
        r'https?://[^\s]+',                          # URLs
        r'[a-z]+\.[a-z]+\.[a-z]+(/[^\s]*)?',        # Domain URLs (admin.omcare.com)
        r'^\d{1,2}/\d{1,2}/\d{2,4}$',               # Dates (MM/DD/YYYY)
        r'^\d{4}-\d{2}-\d{2}$',                      # ISO dates (YYYY-MM-DD)
        r'^Page \d+ of \d+$',                        # Page numbers
        r'^\d+/\d+$',                                # Page ratios (5/16)
        r'^(Home|Back|Next|Previous|Menu|Login|Logout|Sign In|Sign Out)$',  # Navigation
        r'^[A-Z]{2,}$',                              # ALL CAPS short words (likely headers/noise)
        r'^\d+:\d+\s?(AM|PM|am|pm)?$',              # Timestamps
        r'^©.*\d{4}',                                # Copyright notices
        r'^\s*$',                                    # Empty/whitespace only
        r'^[§©®™]+\s*\d*\s*[§©®™]*$',               # Stray symbols (§, ©, ®, ™)
        r'^[}\[\]{}<>]+$',                           # Stray brackets/braces
        r'^\d{3,}$',                                 # Standalone numbers (page numbers)
        r'^[.\-_=]+$',                               # Lines made of dots/dashes
        r'^\w{1,2}$',                                # Single/double letter words (likely OCR errors)
    ]
    
    # Compile patterns for efficiency
    compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in noise_patterns]
    
    for block in blocks:
        text = mask_email_addresses(block.get("text", "").strip())
        block["text"] = text
        block_type = block.get("type", "text")
        layout_type = block.get("layout_type", block_type)

        # Skip empty blocks — but NOT image/table/formula blocks, which are
        # legitimately text-less (an image has no "text"; a table's text is
        # only a convenience rendering of table_rows). Dropping on empty text
        # unconditionally here silently deleted every image ever extracted.
        if not text and block_type not in ("image", "table", "formula") and layout_type not in ("image", "table", "formula"):
            _record("empty_block")
            log.debug(f"NOISE FILTER: Removed empty block")
            continue

        # Text-less structural blocks (an image has no text by definition)
        # skip noise/artifact scrubbing entirely — there's no text to scrub.
        if not text:
            filtered_blocks.append(block)
            continue

        # PRIORITY 4: Skip very short blocks (< 10 chars, likely noise)
        if len(text) < 10 and layout_type not in ["table", "image", "heading", "sub_heading", "page_number"] and block_type not in ["table", "image", "formula"]:
            _record("too_short")
            log.debug(f"NOISE FILTER: Removed short block (<10 chars): '{text[:50]}'")
            continue

        # PRIORITY 4: Check against noise patterns
        is_noise = False
        for pattern in compiled_patterns:
            if pattern.search(text):
                is_noise = True
                _record(f"pattern:{pattern.pattern}")
                log.debug(f"NOISE FILTER: Removed noise pattern '{pattern.pattern}': '{text[:50]}'")
                break

        if not is_noise:
            # Clean inline artifacts from the text
            cleaned_text = clean_inline_artifacts(text)

            # Only keep if cleaning didn't remove everything
            if cleaned_text.strip():
                block["text"] = cleaned_text
                filtered_blocks.append(block)
            else:
                _record("empty_after_cleaning")
                log.debug(f"NOISE FILTER: Text became empty after cleaning: '{text[:50]}'")

    removed_count = len(blocks) - len(filtered_blocks)
    if removed_count > 0:
        log.info(f"NOISE FILTER: Removed {removed_count} noise blocks, kept {len(filtered_blocks)} blocks")
    
    return filtered_blocks


def clean_inline_artifacts(text: str) -> str:
    """
    Clean inline artifacts and noise from text.
    
    FIX 5: Stricter regex patterns to catch all symbol combinations like §3©6, e §6
    
    Removes:
    - Stray symbols (§, ©, ®, ™, }, {, etc.)
    - Symbol-digit combinations (§3, ©6, §3©6)
    - Letter-symbol-digit patterns (e §6)
    - Multiple consecutive spaces
    - Leading/trailing whitespace
    
    Args:
        text: Input text
    
    Returns:
        Cleaned text
    """
    # FIX (clinical-risk): "<" and ">" used to share this character class with
    # true visual artifacts (§©®™{}[]) and were deleted whenever they showed
    # up as an isolated token — e.g. "eGFR < 20 mL/min" lost the "<" and
    # silently became "eGFR 20 mL/min", inverting an eligibility/dosing
    # criterion. "<"/">" are legitimate comparison operators in clinical
    # text, not OCR noise, so they're excluded from every pattern below.
    # FIX 5: Remove isolated symbol-digit combinations (§3, ©6, §3©6)
    text = re.sub(r'\s+[§©®™}\[\]{}]+\d*\s+', ' ', text)  # Space + symbols + optional digits + space
    text = re.sub(r'\s+[§©®™}\[\]{}]+', ' ', text)  # Space + isolated symbols + space

    # FIX 5: Remove letter-symbol-digit patterns (e §6, a ©3)
    text = re.sub(r'\s+\w\s+[§©®™]\s*\d+\s+', ' ', text)  # " e §6 " → " "

    # FIX 5: Remove mixed symbol-digit artifacts anywhere in text (§3©6, }36, {12)
    text = re.sub(r'[§©®™}\[\]{}]+\d+[§©®™}\[\]{}]*', '', text)  # Symbol-digits-optional symbols
    text = re.sub(r'[§©®™}\[\]{}]{2,}', '', text)  # Multiple consecutive symbols

    # FIX 5: Remove isolated brackets that survived previous filters
    text = re.sub(r'\s+[}{\[\]]+\s+', ' ', text)  # Isolated brackets
    
    # FIX 5: Remove isolated copyright/trademark symbols
    text = re.sub(r'\s+[©®™]+\s+', ' ', text)  # Isolated copyright symbols
    
    # FIX 5: Remove stray section symbols not part of valid legal citations
    # Keep valid legal citations like "§ 571.123" but remove stray "§" or "§3"
    text = re.sub(r'§(?!\s*\d+\.\d+)', '', text)  # Remove § not followed by legal section pattern
    
    # Clean up multiple consecutive spaces
    text = re.sub(r'\s{2,}', ' ', text)
    
    # Remove leading/trailing whitespace
    text = text.strip()
    
    return text


def filter_by_content_type(blocks: List[Dict[str, Any]], keep_types: List[str] = None,
                            removal_reasons: Dict[str, int] = None) -> List[Dict[str, Any]]:
    """
    PRIORITY 4: Filter blocks by content type.

    Focus on meaningful content types and skip navigation/header/footer.

    Args:
        blocks: List of content blocks
        keep_types: List of types to keep (default: text, paragraph, title, list_item, table)
        removal_reasons: optional dict to accumulate {reason: count} into —
            shares the same dict passed to filter_noise_blocks() so callers
            get one combined breakdown of why blocks were removed.

    Returns:
        Filtered list of blocks
    """
    if keep_types is None:
        keep_types = ["text", "paragraph", "title", "list_item", "table", "image", "heading", "sub_heading", "page_number"]

    # Types to skip (low-value for RAG)
    skip_types = ["navigation", "header", "footer", "error"]

    filtered_blocks = []

    for block in blocks:
        block_type = block.get("type", "text")
        layout_type = block.get("layout_type", block_type)

        if block_type in skip_types or layout_type in skip_types:
            if removal_reasons is not None:
                reason = f"content_type:{layout_type or block_type}"
                removal_reasons[reason] = removal_reasons.get(reason, 0) + 1
            log.debug(f"CONTENT TYPE FILTER: Skipped {layout_type or block_type} block")
            continue

        filtered_blocks.append(block)

    removed_count = len(blocks) - len(filtered_blocks)
    if removed_count > 0:
        log.info(f"CONTENT TYPE FILTER: Removed {removed_count} low-value blocks")

    return filtered_blocks


def run_unstructured_pipeline(config: dict, ecs_endpoint: str = None) -> None:
    """
    Run unstructured pipeline with simplified architecture:
    S3 → GenericUnstructuredReader → ECSHttpParser → S3JsonWriter
    
    No format detection or format-specific readers needed.
    ECS Docling service handles all 20+ formats.
    
    Args:
        config: Configuration dictionary from config.yaml
        ecs_endpoint: Optional ECS endpoint URL (overrides config if provided via Glue parameters)
    """
    from unstructured_readers.generic_reader import GenericUnstructuredReader
    from parser.ecs_http_parser import ECSHttpParser
    from target.write_cloud import CloudWriter

    log.info("── UNSTRUCTURED PIPELINE ────────────────────────────────────")

    cfg = config["unstructured"]
    src_cfg = cfg["source"]
    tgt_cfg = cfg["target"]
    
    # Get parser config - allow ecs_endpoint parameter to override config
    parser_cfg = cfg.get("parser", {})
    if ecs_endpoint:
        parser_cfg["ecs_endpoint"] = ecs_endpoint
        log.info("Using ECS endpoint from Glue parameter: %s", ecs_endpoint)

    # Thread through the shared top-level settings.retries/retry_delay_seconds
    # (same keys structured_readers/batch_api_read.py already uses) so a
    # transient ECS blip retries instead of permanently failing that file.
    settings_cfg = config.get("settings", {})
    parser_cfg.setdefault("retries", settings_cfg.get("retries", 2))
    parser_cfg.setdefault("retry_delay_seconds", settings_cfg.get("retry_delay_seconds", 60))
    
    region = src_cfg.get("region", "ap-south-1")

    log.info("Source : s3://%s/%s", src_cfg["bucket"], src_cfg.get("prefix", "") or "(root)")
    log.info("Target : s3://%s/%s", tgt_cfg["bucket"], tgt_cfg["output_prefix"])
    log.info("Parser : ECS HTTP (Docling service)")

    # Initialize components
    reader = GenericUnstructuredReader(config)
    parser = ECSHttpParser(parser_cfg)
    writer = CloudWriter({
        "provider": "aws",
        "bucket": tgt_cfg["bucket"],
        "prefix": tgt_cfg["output_prefix"],
        "region": tgt_cfg.get("region", region),
        "file_format": "json",
        "batch_mode": tgt_cfg.get("batch_mode", False),
    })

    stats = {"total": 0, "processed": 0, "skipped": 0, "failed": 0}

    try:
        # Read files from S3 (yields file metadata + bytes)
        for file_record in reader.read():
            stats["total"] += 1
            object_key = file_record["object_key"]
            file_format = file_record["file_format"]
            file_bytes = file_record["file_bytes"]

            log.info(
                "[%d] %s (%s, %d bytes)",
                stats["total"],
                object_key,
                file_format,
                file_record["file_size"]
            )

            try:
                # Parse file via ECS Docling service
                extraction_record = parser.parse(object_key, file_bytes)

                extraction_record["source_uri"] = file_record["source_uri"]
                extraction_record["object_key"] = object_key
                extraction_record["format"] = file_format

                if extraction_record.get("content") is None:
                    log.warning("  Extraction failed: %s", extraction_record.get("error"))
                    stats["failed"] += 1
                else:
                    blocks = extraction_record["content"].get("blocks", [])
                    original_count = len(blocks)

                    log.info(
                        "  Extracted %d block(s): %s",
                        original_count,
                        [b["type"] for b in blocks[:5]]  # Show first 5 block types
                    )

                    # PRIORITY 4: Apply noise filtering. Both stages share one
                    # removal_reasons dict so the final output records WHY
                    # blocks were removed, not just how many — lets you verify
                    # the raw extraction contained what was expected before
                    # this filtering ran, without a separate debug artifact.
                    removal_reasons = {}
                    blocks = filter_noise_blocks(blocks, removal_reasons=removal_reasons)
                    blocks = filter_by_content_type(blocks, removal_reasons=removal_reasons)
                    blocks = reclassify_toc_tables(blocks, removal_reasons=removal_reasons)

                    filtered_count = len(blocks)
                    if filtered_count < original_count:
                        log.info(
                            "  NOISE FILTER: %d → %d blocks (removed %d noise blocks) — reasons: %s",
                            original_count,
                            filtered_count,
                            original_count - filtered_count,
                            removal_reasons
                        )

                    # Update extraction record with filtered blocks
                    extraction_record["content"]["blocks"] = blocks
                    extraction_record["content"]["layout_summary"] = {
                        "table_count": sum(1 for b in blocks if b.get("type") == "table" or b.get("layout_type") == "table"),
                        "paragraph_count": sum(1 for b in blocks if b.get("layout_type") == "paragraph"),
                        "image_count": sum(1 for b in blocks if b.get("type") == "image"),
                        "list_item_count": sum(1 for b in blocks if b.get("layout_type") == "list_item"),
                        "formula_count": sum(1 for b in blocks if b.get("type") == "formula"),
                    }
                    extraction_record["content"]["blocks_original_count"] = original_count
                    extraction_record["content"]["blocks_filtered_count"] = filtered_count
                    extraction_record["content"]["filter_removal_reasons"] = removal_reasons

                    stats["processed"] += 1

                # Write ExtractionRecord to target S3
                writer.write(extraction_record)

            except Exception as exc:
                log.error("  Parse/write error for %s: %s", object_key, exc, exc_info=True)
                stats["failed"] += 1
                # Write error record
                error_record = {
                    "source_uri": file_record["source_uri"],
                    "format": f"unstructured_{file_format}",
                    "extracted_at": file_record["discovered_at"],
                    "content": None,
                    "error": str(exc)
                }
                writer.write(error_record)

    except Exception as exc:
        log.error("Pipeline error: %s", exc, exc_info=True)
        raise
    finally:
        # Cleanup
        reader.close()
        writer.flush()

    log.info(
        "Done — total: %d | processed: %d | skipped: %d | failed: %d",
        stats["total"], stats["processed"], stats["skipped"], stats["failed"]
    )

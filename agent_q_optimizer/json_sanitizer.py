"""
JSON Sanitizer — Strips markdown fences and LLM preamble from model output.
Extracts only the content between the outermost { and } using regex.
"""
import re
import json
import logging

log = logging.getLogger(__name__)


def extract_json(raw: str) -> dict:
    """
    Extract and parse the first valid JSON object from raw LLM output.
    Handles:  ```json ... ```  blocks, 'Here is your JSON:' preambles,
    trailing prose after the closing brace, and Unicode curly quotes.
    Raises ValueError if no valid JSON object is found.
    """
    # Normalize unicode curly quotes that some models emit
    text = raw.replace("\u201c", '"').replace("\u201d", '"')

    # Locate the outermost { ... } pair
    first = text.find('{')
    last  = text.rfind('}')
    if first == -1 or last == -1 or last <= first:
        raise ValueError(f"No JSON object found in LLM output: {raw[:200]!r}")

    candidate = text[first:last + 1]

    try:
        return json.loads(candidate)
    except json.JSONDecodeError as e:
        # Last-resort: strip newline-embedded code-fence artifacts and retry
        cleaned = re.sub(r'```[a-z]*', '', candidate).strip()
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            raise ValueError(
                f"JSON parse failed after sanitization: {e}. "
                f"Candidate: {candidate[:300]!r}"
            )

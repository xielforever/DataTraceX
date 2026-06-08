from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ScriptChunk:
    index: int
    start_line: int
    end_line: int
    text: str


def chunk_script(text: str, max_lines: int = 160, overlap_lines: int = 20) -> list[ScriptChunk]:
    if max_lines <= 0:
        raise ValueError("max_lines must be positive")
    if overlap_lines < 0:
        raise ValueError("overlap_lines must be non-negative")
    if overlap_lines >= max_lines:
        raise ValueError("overlap_lines must be smaller than max_lines")

    lines = text.splitlines()
    if not lines:
        return [ScriptChunk(index=0, start_line=1, end_line=1, text="")]

    chunks: list[ScriptChunk] = []
    start = 0
    index = 0
    step = max_lines - overlap_lines
    while start < len(lines):
        end = min(start + max_lines, len(lines))
        chunks.append(
            ScriptChunk(
                index=index,
                start_line=start + 1,
                end_line=end,
                text="\n".join(lines[start:end]),
            )
        )
        if end == len(lines):
            break
        start += step
        index += 1
    return chunks

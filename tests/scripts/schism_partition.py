#!/usr/bin/env python3
"""Build a Schism-style static assignment CSV from an affinity graph dump."""

from __future__ import annotations

import argparse
import csv
import shutil
import subprocess
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class GraphDump:
    vertices: list[int]
    edges: list[tuple[int, int, int]]
    access: dict[int, int]


def unpack_table_id(tuple_id: int) -> int:
    return (tuple_id >> 48) & 0xFFFF


def unpack_item_key(tuple_id: int) -> int:
    return tuple_id & 0x0000FFFFFFFFFFFF


def load_graph_dump(path: Path) -> GraphDump:
    edge_weights: dict[tuple[int, int], int] = defaultdict(int)
    access: dict[int, int] = defaultdict(int)
    vertices: set[int] = set()

    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row_no, row in enumerate(reader, start=2):
            record_type = (row.get("record_type") or "").strip()
            if record_type == "edge":
                u = int(row["tuple_id_a"])
                v = int(row["tuple_id_b"])
                weight = int(row["weight"])
                if u == v:
                    continue
                if v < u:
                    u, v = v, u
                edge_weights[(u, v)] += weight
                vertices.add(u)
                vertices.add(v)
            elif record_type == "access":
                tuple_id = int(row["tuple_id_a"])
                count = int(row["access_count"])
                access[tuple_id] += count
                vertices.add(tuple_id)
            elif record_type:
                raise ValueError(f"{path}:{row_no}: unknown record_type {record_type!r}")

    edges = [(u, v, w) for (u, v), w in sorted(edge_weights.items())]
    return GraphDump(vertices=sorted(vertices), edges=edges, access=dict(access))


def fallback_partition(vertices: list[int], parts: int) -> dict[int, int]:
    if parts <= 0:
        raise ValueError("parts must be positive")
    return {tuple_id: idx % parts for idx, tuple_id in enumerate(sorted(vertices))}


def write_assignment_csv(path: Path, assignment: dict[int, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["tuple_id", "table_id", "item_key", "node_id"])
        for tuple_id in sorted(assignment):
            writer.writerow([
                tuple_id,
                unpack_table_id(tuple_id),
                unpack_item_key(tuple_id),
                assignment[tuple_id],
            ])


def _write_metis_graph(graph: GraphDump, path: Path) -> list[int]:
    vertices = graph.vertices
    dense = {tuple_id: idx + 1 for idx, tuple_id in enumerate(vertices)}
    adj: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for u, v, weight in graph.edges:
        if u not in dense or v not in dense:
            continue
        u_idx = dense[u]
        v_idx = dense[v]
        adj[u_idx].append((v_idx, weight))
        adj[v_idx].append((u_idx, weight))

    with path.open("w", encoding="utf-8") as fh:
        fh.write(f"{len(vertices)} {len(graph.edges)} 001\n")
        for idx in range(1, len(vertices) + 1):
            fields: list[str] = []
            for nbr, weight in sorted(adj.get(idx, [])):
                fields.extend([str(nbr), str(weight)])
            fh.write(" ".join(fields) + "\n")
    return vertices


def _resolve_gpmetis(binary: str) -> str | None:
    if "/" in binary:
        return binary if Path(binary).exists() else None
    return shutil.which(binary)


def gpmetis_partition(graph: GraphDump, parts: int, gpmetis: str) -> dict[int, int]:
    if parts <= 0:
        raise ValueError("parts must be positive")
    binary = _resolve_gpmetis(gpmetis)
    if binary is None:
        raise FileNotFoundError(f"gpmetis binary not found: {gpmetis}")

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        graph_path = tmp_path / "schism.metis"
        vertices = _write_metis_graph(graph, graph_path)
        subprocess.run(
            [binary, "-seed=1", str(graph_path), str(parts)],
            cwd=tmp_path,
            text=True,
            capture_output=True,
            check=True,
        )
        part_path = tmp_path / f"schism.metis.part.{parts}"
        parts_out = [
            int(line.strip())
            for line in part_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    if len(parts_out) != len(vertices):
        raise RuntimeError(
            f"gpmetis emitted {len(parts_out)} parts for {len(vertices)} vertices"
        )
    return {tuple_id: node_id for tuple_id, node_id in zip(vertices, parts_out)}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert affinity graph dump CSV to Schism static assignment CSV."
    )
    parser.add_argument("--graph", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--parts", type=int, required=True)
    parser.add_argument("--gpmetis", default="gpmetis")
    parser.add_argument(
        "--allow-fallback",
        action="store_true",
        help="Use deterministic round-robin partitioning if gpmetis is unavailable.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    graph = load_graph_dump(args.graph)
    try:
        assignment = gpmetis_partition(graph, args.parts, args.gpmetis)
    except FileNotFoundError:
        if not args.allow_fallback:
            raise
        assignment = fallback_partition(graph.vertices, args.parts)
    write_assignment_csv(args.out, assignment)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"schism_partition: {exc}", file=sys.stderr)
        raise SystemExit(1)

"""Instance family metadata — single source of truth for category, arch, generation.

Used by recommender.py to find cost-optimal alternatives within the same
workload category at the same or newer generation.
"""
from __future__ import annotations

from typing import Literal, NamedTuple

Arch = Literal['intel', 'amd', 'graviton']
Category = Literal[
    'general_burstable',  # t-family
    'general',            # m-family
    'compute',            # c-family
    'memory',             # r-family
    'memory_intensive',   # x / z / u families
    'storage',            # i / d / im / is / h families
    'accelerated_gpu',    # g / p families
    'accelerated_ml',     # inf / trn families
    'hpc',                # hpc families
    'specialty',          # f1, vt1 — no upgrade path
]


class FamilyInfo(NamedTuple):
    category: Category
    arch: Arch
    gen: int


# ---------------------------------------------------------------------------
# Every family present in the bundled EC2 pricing dataset (eu-west-1).
# Families NOT listed here get no recommendation (N/A) — fail-safe.
# ---------------------------------------------------------------------------
FAMILY_META: dict[str, FamilyInfo] = {
    # ── T-family (general purpose, burstable) ──────────────────────────────
    't2':        FamilyInfo('general_burstable', 'intel',   2),
    't3':        FamilyInfo('general_burstable', 'intel',   3),
    't3a':       FamilyInfo('general_burstable', 'amd',     3),
    't4g':       FamilyInfo('general_burstable', 'graviton', 4),

    # ── M-family (general purpose) ─────────────────────────────────────────
    'm4':        FamilyInfo('general', 'intel',   4),
    'm5':        FamilyInfo('general', 'intel',   5),
    'm5a':       FamilyInfo('general', 'amd',     5),
    'm5ad':      FamilyInfo('general', 'amd',     5),
    'm5d':       FamilyInfo('general', 'intel',   5),
    'm5dn':      FamilyInfo('general', 'intel',   5),
    'm5n':       FamilyInfo('general', 'intel',   5),
    'm5zn':      FamilyInfo('general', 'intel',   5),
    'm6a':       FamilyInfo('general', 'amd',     6),
    'm6g':       FamilyInfo('general', 'graviton', 6),
    'm6gd':      FamilyInfo('general', 'graviton', 6),
    'm6i':       FamilyInfo('general', 'intel',   6),
    'm6id':      FamilyInfo('general', 'intel',   6),
    'm6idn':     FamilyInfo('general', 'intel',   6),
    'm6in':      FamilyInfo('general', 'intel',   6),
    'm7a':       FamilyInfo('general', 'amd',     7),
    'm7g':       FamilyInfo('general', 'graviton', 7),
    'm7gd':      FamilyInfo('general', 'graviton', 7),
    'm7i':       FamilyInfo('general', 'intel',   7),
    'm7i-flex':  FamilyInfo('general', 'intel',   7),
    'm8a':       FamilyInfo('general', 'amd',     8),
    'm8g':       FamilyInfo('general', 'graviton', 8),
    'm8gd':      FamilyInfo('general', 'graviton', 8),
    'm8i':       FamilyInfo('general', 'intel',   8),
    'm8i-flex':  FamilyInfo('general', 'intel',   8),

    # ── C-family (compute optimised) ───────────────────────────────────────
    'c4':        FamilyInfo('compute', 'intel',   4),
    'c5':        FamilyInfo('compute', 'intel',   5),
    'c5a':       FamilyInfo('compute', 'amd',     5),
    'c5ad':      FamilyInfo('compute', 'amd',     5),
    'c5d':       FamilyInfo('compute', 'intel',   5),
    'c5n':       FamilyInfo('compute', 'intel',   5),
    'c6a':       FamilyInfo('compute', 'amd',     6),
    'c6g':       FamilyInfo('compute', 'graviton', 6),
    'c6gd':      FamilyInfo('compute', 'graviton', 6),
    'c6gn':      FamilyInfo('compute', 'graviton', 6),
    'c6i':       FamilyInfo('compute', 'intel',   6),
    'c6id':      FamilyInfo('compute', 'intel',   6),
    'c6in':      FamilyInfo('compute', 'intel',   6),
    'c7a':       FamilyInfo('compute', 'amd',     7),
    'c7g':       FamilyInfo('compute', 'graviton', 7),
    'c7gd':      FamilyInfo('compute', 'graviton', 7),
    'c7gn':      FamilyInfo('compute', 'graviton', 7),
    'c7i':       FamilyInfo('compute', 'intel',   7),
    'c7i-flex':  FamilyInfo('compute', 'intel',   7),
    'c8a':       FamilyInfo('compute', 'amd',     8),
    'c8g':       FamilyInfo('compute', 'graviton', 8),
    'c8gd':      FamilyInfo('compute', 'graviton', 8),
    'c8gn':      FamilyInfo('compute', 'graviton', 8),

    # ── R-family (memory optimised) ────────────────────────────────────────
    'r4':        FamilyInfo('memory', 'intel',   4),
    'r5':        FamilyInfo('memory', 'intel',   5),
    'r5a':       FamilyInfo('memory', 'amd',     5),
    'r5ad':      FamilyInfo('memory', 'amd',     5),
    'r5b':       FamilyInfo('memory', 'intel',   5),
    'r5d':       FamilyInfo('memory', 'intel',   5),
    'r5dn':      FamilyInfo('memory', 'intel',   5),
    'r5n':       FamilyInfo('memory', 'intel',   5),
    'r6a':       FamilyInfo('memory', 'amd',     6),
    'r6g':       FamilyInfo('memory', 'graviton', 6),
    'r6gd':      FamilyInfo('memory', 'graviton', 6),
    'r6i':       FamilyInfo('memory', 'intel',   6),
    'r6id':      FamilyInfo('memory', 'intel',   6),
    'r6idn':     FamilyInfo('memory', 'intel',   6),
    'r6in':      FamilyInfo('memory', 'intel',   6),
    'r7a':       FamilyInfo('memory', 'amd',     7),
    'r7g':       FamilyInfo('memory', 'graviton', 7),
    'r7gd':      FamilyInfo('memory', 'graviton', 7),
    'r7i':       FamilyInfo('memory', 'intel',   7),
    'r7iz':      FamilyInfo('memory', 'intel',   7),
    'r8a':       FamilyInfo('memory', 'amd',     8),
    'r8g':       FamilyInfo('memory', 'graviton', 8),
    'r8gd':      FamilyInfo('memory', 'graviton', 8),
    'r8i':       FamilyInfo('memory', 'intel',   8),
    'r8i-flex':  FamilyInfo('memory', 'intel',   8),

    # ── I-family (storage optimised) ───────────────────────────────────────
    'i3':        FamilyInfo('storage', 'intel',   3),
    'i3en':      FamilyInfo('storage', 'intel',   3),
    'i4g':       FamilyInfo('storage', 'graviton', 4),
    'i4i':       FamilyInfo('storage', 'intel',   4),
    'i7i':       FamilyInfo('storage', 'intel',   7),
    'i7ie':      FamilyInfo('storage', 'intel',   7),
    'i8g':       FamilyInfo('storage', 'graviton', 8),
    'i8ge':      FamilyInfo('storage', 'graviton', 8),
    'im4gn':     FamilyInfo('storage', 'graviton', 4),
    'is4gen':    FamilyInfo('storage', 'graviton', 4),

    # ── D-family (dense / HDD storage) ─────────────────────────────────────
    'd3':        FamilyInfo('storage', 'intel',   3),
    'd3en':      FamilyInfo('storage', 'intel',   3),
    'h1':        FamilyInfo('storage', 'intel',   1),

    # ── X-family (memory intensive) ────────────────────────────────────────
    'x1':        FamilyInfo('memory_intensive', 'intel',   1),
    'x1e':       FamilyInfo('memory_intensive', 'intel',   1),
    'x2gd':      FamilyInfo('memory_intensive', 'graviton', 2),
    'x2idn':     FamilyInfo('memory_intensive', 'intel',   2),
    'x2iedn':    FamilyInfo('memory_intensive', 'intel',   2),
    'x2iezn':    FamilyInfo('memory_intensive', 'intel',   2),

    # ── U-family (high memory) ─────────────────────────────────────────────
    'u-3tb1':    FamilyInfo('memory_intensive', 'intel',   1),
    'u-6tb1':    FamilyInfo('memory_intensive', 'intel',   1),
    'u7i-12tb':  FamilyInfo('memory_intensive', 'intel',   7),
    'u7i-6tb':   FamilyInfo('memory_intensive', 'intel',   7),
    'u7i-8tb':   FamilyInfo('memory_intensive', 'intel',   7),
    'u7in-16tb': FamilyInfo('memory_intensive', 'intel',   7),

    # ── Z-family (high frequency) ──────────────────────────────────────────
    'z1d':       FamilyInfo('memory_intensive', 'intel',   1),

    # ── P-family (GPU — training) ──────────────────────────────────────────
    'p2':        FamilyInfo('accelerated_gpu', 'intel',  2),
    'p3':        FamilyInfo('accelerated_gpu', 'intel',  3),
    'p3dn':      FamilyInfo('accelerated_gpu', 'intel',  3),
    'p4d':       FamilyInfo('accelerated_gpu', 'intel',  4),

    # ── G-family (GPU — graphics / inference) ──────────────────────────────
    'g3':        FamilyInfo('accelerated_gpu', 'intel',  3),
    'g3s':       FamilyInfo('accelerated_gpu', 'intel',  3),
    'g4ad':      FamilyInfo('accelerated_gpu', 'amd',    4),
    'g4dn':      FamilyInfo('accelerated_gpu', 'intel',  4),
    'g5':        FamilyInfo('accelerated_gpu', 'amd',    5),

    # ── Inference / ML ─────────────────────────────────────────────────────
    'inf1':      FamilyInfo('accelerated_ml', 'intel',   1),
    'inf2':      FamilyInfo('accelerated_ml', 'intel',   2),

    # ── HPC ────────────────────────────────────────────────────────────────
    'hpc7a':     FamilyInfo('hpc', 'amd',     7),
    'hpc7g':     FamilyInfo('hpc', 'graviton', 7),

    # ── Specialty (no upgrade path — will return N/A) ──────────────────────
    'f1':        FamilyInfo('specialty', 'intel',   1),
    'vt1':       FamilyInfo('specialty', 'intel',   1),
}

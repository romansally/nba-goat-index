"""Reference-artifact drift guard — CLAUDE.md Rule 1, input-domain tightening
condition 3 (ADR-0003 "Baseline for condition 3").

The protected artifacts — data/seed/, tests/fixtures/, tests/golden/ — must be
unchanged across THE ENTIRE CHANGE, defined as the full diff against the target
branch's merge base, never any single commit: commit boundaries are
author-controlled (the round-5 Codex exploit). Hardened twice since: round 6
closed delete/re-add pin resets, non-JSON escapes, and criss-cross merge bases;
round 7 closed the INDEX gap — `git commit` records staged bytes, not working
bytes, so every comparison here verifies BOTH the working tree and the
staged index entry (including file mode — a regular-file-to-symlink swap with
identical apparent content is a mode change), and any untracked file under a
protected tree is refused outright (a new artifact could otherwise be staged
after a passing check without detection); round 10 closed the mirror image,
the COMMITTED-HEAD gap — a merge records HEAD's tree, not the index or the
working tree, so tampered bytes committed at HEAD behind a safe
index/worktree restoration passed every earlier leg while the merge would
still carry the malicious committed bytes.

The five obligations:

1. data/seed/ and tests/fixtures/: zero diff from the merge base in the
   working tree (`git diff <base>`), the index (`git diff --cached <base>`),
   AND the committed HEAD tree (`git diff <base> HEAD` — round-10 fix 1).
2. No untracked files anywhere under the protected trees — IGNORED OR NOT
   (round-8 fix 2: --exclude-standard would let a .gitignore entry hide one).
3. Every tracked file under tests/golden/ (any extension; only the .gitkeep
   metadata basename is exempt) that exists at the merge base: committed HEAD
   entry (round-10 fix 2), staged entry (mode + blob), and working bytes ALL
   equal the merge-base entry; never deleted, never a symlink.
4. A golden absent from the merge base must be GENUINELY NEW and PINNED: no
   history reachable from the merge base, exactly ONE add and ZERO deletes in
   merge_base..HEAD, EVERY commit on the ancestry path from the creation
   commit to HEAD recording exactly the creation entry (round-10 fix 3:
   add/delete events never inspected MODIFY commits, so a modify-then-restore
   pair laundered intermediate bytes), and HEAD + staged entry + working
   bytes equal to the sole creation commit's entry — and the candidate set is
   every golden path TOUCHED
   anywhere in merge_base..HEAD (rename detection disabled, NUL-delimited),
   unioned with the current index, not just paths still visible at the tip
   (round-8 fix 1: a golden created then deleted mid-branch belonged to no
   checked set and evaded every guard). Every path-limited history query runs
   with --full-history: default history simplification follows only one
   TREESAME parent at each merge, so a merged side branch's create/delete/
   reintroduce history was invisible to all of these queries — the round-8
   exploit transported through an ordinary merge (round-9 severe fix).
5. The merge base itself resolves strictly: shallow repositories rejected,
   `merge-base --all` must yield exactly one base.

The byte invariant is RAW PHYSICAL bytes: golden hashing uses
`hash-object --no-filters`, so a .gitattributes clean filter cannot make
physically different working bytes hash "equal" (round-8 boundary note,
addressed rather than documented — the pipeline reads physical bytes, so
physical equality is the invariant that matters; an autocrlf-mutated checkout
therefore fails loudly, which is correct for byte-exact reference data). The
seed/fixtures legs still ride on `git diff`, which respects filters — that
residual, like skip-worktree/assume-unchanged index bits and GIT_* env
redirection, is a LOCAL-run distortion inside the documented threat boundary:
it cannot place tampered bytes into main without appearing in the reviewed
merge diff.

On main itself the merge base is HEAD and the checks degrade to "no staged or
unstaged reference edits" — still the right invariant. The bypass-regression
tests build throwaway git repositories and prove every closed attack fails:
delete/re-add laundering, removed-before-merge-base laundering, non-JSON
tampering, criss-cross bases, shallow clones, staged-vs-working splits (seed
and golden), symlink mode swaps, untracked drops, merge-hidden side-branch
golden histories, committed-HEAD tampering behind a restored index/worktree
(seed and golden), and modify-then-restore creation laundering.
"""

import os
import subprocess
from pathlib import Path

import pytest

PROTECTED_TREES = ["data/seed", "tests/fixtures"]
GOLDEN_DIR = "tests/golden"
# Metadata files that are not reference content (basenames).
GOLDEN_ALLOWLIST = {".gitkeep"}


class ReferenceArtifactViolation(AssertionError):
    """A CLAUDE.md Rule 1 condition-3 obligation failed."""


def _git(*args: str, cwd: Path | None = None) -> str:
    result = subprocess.run(
        ["git", *args], capture_output=True, text=True, check=True, cwd=cwd
    )
    return result.stdout.strip()


def resolve_merge_base(repo: Path, target: str = "main") -> str:
    """The condition-3 baseline, resolved strictly (fail loud, never guess)."""
    if _git("rev-parse", "--is-shallow-repository", cwd=repo) != "false":
        raise ReferenceArtifactViolation(
            "shallow repository: the reference-artifact guard needs FULL git "
            "history to verify creation pins and merge bases — fetch with "
            "full depth (git fetch --unshallow) before running the gate"
        )
    bases = _git("merge-base", "--all", target, "HEAD", cwd=repo).splitlines()
    if len(bases) != 1:
        raise ReferenceArtifactViolation(
            f"expected exactly one merge base with {target}, got "
            f"{len(bases)} ({bases}): a criss-cross history could hide drift "
            "relative to whichever base was not checked — linearize first"
        )
    return bases[0]


def _goldens_at(commit: str, repo: Path) -> set[str]:
    listing = _git("ls-tree", "-r", "--name-only", commit, GOLDEN_DIR, cwd=repo)
    return {p for p in listing.splitlines() if Path(p).name not in GOLDEN_ALLOWLIST}


def _tracked_goldens(repo: Path) -> list[str]:
    listing = _git("ls-files", GOLDEN_DIR, cwd=repo)
    return [p for p in listing.splitlines() if Path(p).name not in GOLDEN_ALLOWLIST]


def _goldens_touched(merge_base: str, repo: Path) -> set[str]:
    """Every golden path any commit in merge_base..HEAD touched — rename
    detection disabled so renames appear as explicit delete/add pairs, and
    NUL-delimited so nothing hides in formatting (round-8 Codex fix 1: a
    created-then-deleted golden is invisible at both the merge base and the
    tip, so the candidate set must come from history, not tree listings).
    --full-history: without it, path-limited log follows only one TREESAME
    parent at each merge, hiding a merged side branch's create/delete
    (round-9 Codex fix)."""
    out = _git(
        "log",
        "--full-history",
        "--format=",
        "--name-only",
        "-z",
        "--no-renames",
        f"{merge_base}..HEAD",
        "--",
        GOLDEN_DIR,
        cwd=repo,
    )
    return {p for p in out.split("\0") if p and Path(p).name not in GOLDEN_ALLOWLIST}


def _tree_entry(commit: str, path: str, repo: Path) -> tuple[str, str] | None:
    """(mode, blob) as recorded in a commit's tree, or None if absent."""
    out = _git("ls-tree", commit, "--", path, cwd=repo)
    if not out:
        return None
    mode, _kind, blob = out.split()[:3]
    return mode, blob


def _index_entry(path: str, repo: Path) -> tuple[str, str] | None:
    """(mode, blob) as currently STAGED — what `git commit` would record."""
    out = _git("ls-files", "--stage", "--", path, cwd=repo)
    if not out:
        return None
    mode, blob = out.split()[:2]
    return mode, blob


def _verify_golden(
    path: str, expected: tuple[str, str], source: str, repo: Path
) -> None:
    """A golden must match its expected (mode, blob) in the committed HEAD
    tree, the index, AND the working tree: `git commit` records the index, so
    working-tree-only checks are launderable by staging bad bytes and
    restoring the file (round-7 Codex exploit); a MERGE records HEAD's tree,
    so index+worktree-only checks are launderable by committing bad bytes and
    restoring both (round-10 Codex exploit, the mirror image); the working
    tree is what the pipeline actually reads."""
    expected_mode, expected_blob = expected
    if expected_mode != "100644":
        raise ReferenceArtifactViolation(
            f"{path}: expected entry has mode {expected_mode} — only plain "
            "regular files are valid reference artifacts (no symlinks, no "
            "executable bits)"
        )
    committed = _tree_entry("HEAD", path, repo)
    if committed != expected:
        found = (
            f"mode {committed[0]}, blob {committed[1][:12]}" if committed else "absent"
        )
        raise ReferenceArtifactViolation(
            f"{path}: committed HEAD tree entry ({found}) differs from "
            f"{source} (mode {expected_mode}, blob {expected_blob[:12]}) — a "
            "merge of HEAD carries the COMMITTED bytes regardless of any "
            "index/working-tree restoration (round-10 Codex fix, mirror "
            "image of the round-7 index gap)"
        )
    staged = _index_entry(path, repo)
    if staged is None:
        raise ReferenceArtifactViolation(
            f"{path}: missing from the index — reference artifacts cannot be "
            "unstaged/removed inside a change"
        )
    if staged != expected:
        raise ReferenceArtifactViolation(
            f"{path}: staged index entry (mode {staged[0]}, blob "
            f"{staged[1][:12]}) differs from {source} (mode {expected_mode}, "
            f"blob {expected_blob[:12]}) — the next commit would record the "
            "staged version regardless of working-tree content (mode changes "
            "such as a regular-file-to-symlink swap are rejected here too)"
        )
    file = repo / path
    if file.is_symlink():
        raise ReferenceArtifactViolation(
            f"{path}: working-tree path is a symlink — mode change; only "
            "plain regular files are valid reference artifacts"
        )
    if not file.is_file():
        raise ReferenceArtifactViolation(
            f"{path}: missing from the working tree — reference artifacts "
            "cannot be removed inside a change (golden changes take the "
            "version-bump path, Rule 5)"
        )
    # --no-filters: raw physical bytes, immune to .gitattributes clean
    # filters that could normalize tampered content back to the expected
    # blob (round-8 boundary note — the pipeline reads physical bytes).
    working_blob = _git("hash-object", "--no-filters", path, cwd=repo)
    if working_blob != expected_blob:
        raise ReferenceArtifactViolation(
            f"{path}: working tree differs from {source} — golden snapshots "
            "change only via the full version-bump path (CLAUDE.md Rule 5)"
        )


def check_seed_and_fixtures(merge_base: str, repo: Path) -> None:
    """Zero drift in the working tree, the index, AND the committed HEAD tree
    (round-7 fix 1 + round-10 fix 1: the first two legs compare working and
    staged bytes, so tampered bytes COMMITTED at HEAD behind a safe
    index/worktree restoration passed both — yet a merge of HEAD carries the
    committed bytes, the mirror image of the round-7 index gap)."""
    legs = (
        ("working tree", (merge_base,)),
        ("index", ("--cached", merge_base)),
        ("committed HEAD tree", (merge_base, "HEAD")),
    )
    for where, args in legs:
        diff = _git("diff", *args, "--stat", "--", *PROTECTED_TREES, cwd=repo)
        if diff:
            raise ReferenceArtifactViolation(
                f"protected reference artifacts drifted from merge base "
                f"{merge_base[:12]} in the {where} (ADR-0003 condition-3 "
                f"baseline; git commit records the index, a merge records "
                f"HEAD):\n{diff}"
            )


def check_no_untracked_under_protected(repo: Path) -> None:
    """An untracked file under a protected tree could be staged the moment
    after a passing check — refuse it outright (round-7 fix 4). No
    --exclude-standard: a .gitignore entry must not be able to hide one
    (round-8 fix 2) — "any untracked file" means ignored ones too."""
    untracked = _git(
        "ls-files",
        "--others",
        "--",
        *PROTECTED_TREES,
        GOLDEN_DIR,
        cwd=repo,
    )
    if untracked:
        raise ReferenceArtifactViolation(
            "untracked file(s) under protected reference trees (ignored or "
            "not) — a stage-after-check laundering channel:\n" + untracked
        )


def check_merge_base_goldens(merge_base: str, repo: Path) -> None:
    """Goldens existing at the merge base: committed HEAD, staged, and
    working entries ALL equal the merge-base entry, mode included
    (round-6 fix 2 + round-7 fix 2 + round-10 fix 2)."""
    for path in sorted(_goldens_at(merge_base, repo)):
        expected = _tree_entry(merge_base, path, repo)
        _verify_golden(path, expected, f"the merge base {merge_base[:12]}", repo)


def check_created_goldens(merge_base: str, repo: Path) -> None:
    """Goldens absent from the merge base: genuinely new, pinned in history,
    and identical in index + working tree to the sole creation commit
    (round-6 fix 1 + round-7 fix 3 + round-8 fix 1). The candidate set is the
    UNION of the current index and every golden path touched in
    merge_base..HEAD — a created-then-deleted golden appears in neither tree
    listing and must still be inspected. All history queries here run
    --full-history so merged side branches cannot hide (round-9 fix)."""
    at_base = _goldens_at(merge_base, repo)
    candidates = (
        set(_tracked_goldens(repo)) | _goldens_touched(merge_base, repo)
    ) - at_base
    for path in sorted(candidates):
        prior = _git(
            "log",
            "--full-history",
            "--format=%H",
            "-n",
            "1",
            merge_base,
            "--",
            path,
            cwd=repo,
        )
        if prior:
            raise ReferenceArtifactViolation(
                f"{path}: absent from the merge base but present in history "
                "reachable from it (existed and was removed) — not genuinely "
                "new; a prior version exists to launder"
            )
        span = f"{merge_base}..HEAD"
        adds = _git(
            "log",
            "--full-history",
            "--diff-filter=A",
            "--format=%H",
            "--no-renames",
            span,
            "--",
            path,
            cwd=repo,
        ).splitlines()
        deletes = _git(
            "log",
            "--full-history",
            "--diff-filter=D",
            "--format=%H",
            "--no-renames",
            span,
            "--",
            path,
            cwd=repo,
        ).splitlines()
        if len(adds) != 1 or deletes:
            raise ReferenceArtifactViolation(
                f"{path}: expected exactly one add and zero deletes in "
                f"{span}, found {len(adds)} add(s) and {len(deletes)} "
                "delete(s) — a created reference artifact must appear exactly "
                "once and survive to the tip: delete/re-add resets the "
                "creation pin, and created-then-deleted contradicts 'pinned "
                "from creation through the tip' (round-6 + round-8 Codex fixes)"
            )
        expected = _tree_entry(adds[0], path, repo)
        # Round-10 fix 3: add/delete events never inspected MODIFY commits —
        # a modify-then-restore pair leaves the tip equal to the creation
        # entry while intermediate commits carried different bytes. "Pinned
        # from creation through the tip" means EVERY commit, so every commit
        # on the ancestry path from creation to HEAD must record the creation
        # entry exactly (rev-list is not path-limited, so history
        # simplification cannot hide commits here).
        ancestry = _git(
            "rev-list", "--ancestry-path", f"{adds[0]}..HEAD", cwd=repo
        ).splitlines()
        for commit in ancestry:
            entry = _tree_entry(commit, path, repo)
            if entry != expected:
                found = f"mode {entry[0]}, blob {entry[1][:12]}" if entry else "absent"
                raise ReferenceArtifactViolation(
                    f"{path}: commit {commit[:12]} between the creation "
                    f"commit and HEAD records a different entry ({found}) — "
                    "'pinned from creation through the tip' means EVERY "
                    "commit; a modify-then-restore pair must not launder "
                    "intermediate bytes (round-10 Codex fix)"
                )
        _verify_golden(path, expected, f"its creation commit {adds[0][:7]}", repo)


# ------------------------------------------------ the guard, on THIS repo
REPO = Path(".")


@pytest.fixture(scope="module")
def merge_base() -> str:
    return resolve_merge_base(REPO)


def test_seed_and_fixtures_match_merge_base(merge_base):
    check_seed_and_fixtures(merge_base, REPO)


def test_no_untracked_under_protected_trees():
    check_no_untracked_under_protected(REPO)


def test_merge_base_goldens_unchanged(merge_base):
    check_merge_base_goldens(merge_base, REPO)


def test_created_goldens_genuinely_new_and_pinned(merge_base):
    check_created_goldens(merge_base, REPO)


# ------------------- bypass regressions, on throwaway git histories
def _init_repo(root: Path) -> Path:
    root.mkdir()
    _git("init", "-q", "-b", "main", cwd=root)
    _git("config", "user.email", "guard@test", cwd=root)
    _git("config", "user.name", "guard", cwd=root)
    _git("config", "commit.gpgsign", "false", cwd=root)
    return root


def _commit_all(repo: Path, message: str) -> str:
    _git("add", "-A", cwd=repo)
    _git("commit", "-q", "-m", message, cwd=repo)
    return _git("rev-parse", "HEAD", cwd=repo)


def _write(repo: Path, rel: str, content: str) -> None:
    path = repo / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def test_delete_readd_laundering_fails(tmp_path):
    """Round-6 fix 1: create -> delete -> re-add-with-different-bytes defeated
    the old most-recent-add pin; the full-history rule must refuse it."""
    repo = _init_repo(tmp_path / "repo")
    _write(repo, "README.md", "base\n")
    _commit_all(repo, "base")
    _git("checkout", "-q", "-b", "feature", cwd=repo)
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", '{"v": 1}\n')
    _commit_all(repo, "add golden")
    (repo / GOLDEN_DIR / "vX_scores.json").unlink()
    _commit_all(repo, "delete golden")
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", '{"v": 2}\n')
    _commit_all(repo, "re-add golden, different bytes")
    base = resolve_merge_base(repo)
    with pytest.raises(ReferenceArtifactViolation, match="delete/re-add"):
        check_created_goldens(base, repo)


def test_removed_before_merge_base_is_not_genuinely_new(tmp_path):
    """Round-6 fix 1, second shape: a path that existed on main and was
    removed BEFORE the merge base has a prior version to launder — re-adding
    it inside a change must not qualify for the created-here allowance."""
    repo = _init_repo(tmp_path / "repo")
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", '{"v": "old"}\n')
    _commit_all(repo, "golden once existed on main")
    (repo / GOLDEN_DIR / "vX_scores.json").unlink()
    _commit_all(repo, "golden removed on main")
    _git("checkout", "-q", "-b", "feature", cwd=repo)
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", '{"v": "laundered"}\n')
    _commit_all(repo, "re-add inside the change")
    base = resolve_merge_base(repo)
    with pytest.raises(ReferenceArtifactViolation, match="not genuinely new"):
        check_created_goldens(base, repo)


def test_non_json_golden_is_protected(tmp_path):
    """Round-6 fix 2: Rule 1 protects everything under tests/golden/, not just
    *.json — modifying a .csv golden must fail the merge-base check (while the
    .gitkeep allowlist stays exempt)."""
    repo = _init_repo(tmp_path / "repo")
    _write(repo, f"{GOLDEN_DIR}/.gitkeep", "")
    _write(repo, f"{GOLDEN_DIR}/example.csv", "a,b\n1,2\n")
    _commit_all(repo, "non-json golden on main")
    _git("checkout", "-q", "-b", "feature", cwd=repo)
    _write(repo, f"{GOLDEN_DIR}/example.csv", "a,b\n1,999\n")
    _commit_all(repo, "tamper the csv golden")
    base = resolve_merge_base(repo)
    with pytest.raises(ReferenceArtifactViolation, match="example.csv"):
        check_merge_base_goldens(base, repo)


def test_multiple_merge_bases_rejected(tmp_path):
    """Round-6 fix 3: a criss-cross history yields multiple merge bases and
    `git merge-base` would silently pick one — the guard must refuse."""
    repo = _init_repo(tmp_path / "repo")
    _write(repo, "root.txt", "root\n")
    _commit_all(repo, "root")
    _write(repo, "a.txt", "a\n")
    sha_a = _commit_all(repo, "A on main")
    _git("checkout", "-q", "-b", "q", "HEAD~1", cwd=repo)
    _write(repo, "b.txt", "b\n")
    sha_b = _commit_all(repo, "B on q")
    _git("checkout", "-q", "main", cwd=repo)
    _git("merge", "-q", "--no-edit", sha_b, cwd=repo)
    _git("checkout", "-q", "q", cwd=repo)
    _git("merge", "-q", "--no-edit", sha_a, cwd=repo)
    with pytest.raises(ReferenceArtifactViolation, match="one merge base"):
        resolve_merge_base(repo)


def test_shallow_repository_rejected(tmp_path):
    """Round-6 'also': shallow clones lack the history the creation-pin and
    merge-base checks depend on — reject explicitly, never silently pass."""
    src = _init_repo(tmp_path / "src")
    _write(src, "one.txt", "1\n")
    _commit_all(src, "one")
    _write(src, "two.txt", "2\n")
    _commit_all(src, "two")
    shallow = tmp_path / "shallow"
    _git("clone", "-q", "--depth", "1", f"file://{src.resolve()}", str(shallow))
    with pytest.raises(ReferenceArtifactViolation, match="shallow"):
        resolve_merge_base(shallow)


def test_staged_seed_bytes_caught_despite_clean_working_tree(tmp_path):
    """Round-7 exploit, seed shape: stage malicious bytes, restore the safe
    working file WITHOUT updating the index. `git diff <base>` is clean, but
    the next commit records the staged version — the --cached leg must fail."""
    repo = _init_repo(tmp_path / "repo")
    safe = "player_id,name\n1,safe\n"
    _write(repo, "data/seed/players.csv", safe)
    _commit_all(repo, "seed on main")
    _git("checkout", "-q", "-b", "feature", cwd=repo)
    _write(repo, "data/seed/players.csv", "player_id,name\n1,malicious\n")
    _git("add", "data/seed/players.csv", cwd=repo)
    _write(repo, "data/seed/players.csv", safe)  # working tree restored
    base = resolve_merge_base(repo)
    with pytest.raises(ReferenceArtifactViolation, match="index"):
        check_seed_and_fixtures(base, repo)


def test_staged_golden_bytes_caught_despite_pinned_working_tree(tmp_path):
    """Round-7 exploit, golden shape: stage a tampered golden, restore the
    pinned bytes to the working tree — the staged index entry must fail."""
    repo = _init_repo(tmp_path / "repo")
    pinned = '{"goat_score": 57.58}\n'
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", pinned)
    _commit_all(repo, "golden on main")
    _git("checkout", "-q", "-b", "feature", cwd=repo)
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", '{"goat_score": 99.99}\n')
    _git("add", f"{GOLDEN_DIR}/vX_scores.json", cwd=repo)
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", pinned)  # working restored
    base = resolve_merge_base(repo)
    with pytest.raises(ReferenceArtifactViolation, match="staged"):
        check_merge_base_goldens(base, repo)


def test_symlink_mode_swap_rejected(tmp_path):
    """Round-7 fix 2: replacing a regular golden with a symlink to a file of
    identical apparent content is a MODE change (100644 -> 120000) and must be
    rejected even though reading through the link yields the same bytes."""
    repo = _init_repo(tmp_path / "repo")
    content = '{"goat_score": 57.58}\n'
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", content)
    _commit_all(repo, "golden on main")
    _git("checkout", "-q", "-b", "feature", cwd=repo)
    _write(repo, "decoy.json", content)  # identical apparent content
    (repo / GOLDEN_DIR / "vX_scores.json").unlink()
    os.symlink("../decoy.json", repo / GOLDEN_DIR / "vX_scores.json")
    _git("add", "-A", cwd=repo)
    base = resolve_merge_base(repo)
    with pytest.raises(ReferenceArtifactViolation, match="mode"):
        check_merge_base_goldens(base, repo)


def test_untracked_file_under_protected_tree_caught(tmp_path):
    """Round-7 fix 4: an untracked file under a protected tree is a
    stage-after-check channel — refused outright, before it is ever staged."""
    repo = _init_repo(tmp_path / "repo")
    _write(repo, "README.md", "base\n")
    _commit_all(repo, "base")
    _git("checkout", "-q", "-b", "feature", cwd=repo)
    _write(repo, "data/seed/smuggled.csv", "x\n")  # never staged
    with pytest.raises(ReferenceArtifactViolation, match="untracked"):
        check_no_untracked_under_protected(repo)


def test_created_then_deleted_golden_is_rejected(tmp_path):
    """Round-8 SEVERE: a golden created after the merge base and deleted
    before the tip is tracked at neither the base nor the tip, so the old
    candidate set never inspected it — contradicting 'pinned from creation
    through the tip'. The history-derived candidate set must refuse it."""
    repo = _init_repo(tmp_path / "repo")
    _write(repo, "README.md", "base\n")
    _commit_all(repo, "base")
    _git("checkout", "-q", "-b", "feature", cwd=repo)
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", '{"v": "transient"}\n')
    _commit_all(repo, "add golden mid-branch")
    (repo / GOLDEN_DIR / "vX_scores.json").unlink()
    _commit_all(repo, "delete it before the tip — no re-add")
    base = resolve_merge_base(repo)
    with pytest.raises(ReferenceArtifactViolation, match="survive to the tip"):
        check_created_goldens(base, repo)


def test_ignored_untracked_protected_file_is_rejected(tmp_path):
    """Round-8 fix 2: --exclude-standard let a .gitignore entry hide an
    untracked file from the untracked check — 'any untracked file' must mean
    ignored ones too, under every protected tree."""
    repo = _init_repo(tmp_path / "repo")
    _write(repo, "README.md", "base\n")
    _write(
        repo,
        ".gitignore",
        "data/seed/hidden.csv\ntests/fixtures/hidden.csv\ntests/golden/hidden.json\n",
    )
    _commit_all(repo, "base with ignore rules")
    _git("checkout", "-q", "-b", "feature", cwd=repo)
    _write(repo, "data/seed/hidden.csv", "smuggled\n")
    _write(repo, "tests/fixtures/hidden.csv", "smuggled\n")
    _write(repo, f"{GOLDEN_DIR}/hidden.json", '{"v": "smuggled"}\n')
    with pytest.raises(ReferenceArtifactViolation, match="untracked") as excinfo:
        check_no_untracked_under_protected(repo)
    for path in (
        "data/seed/hidden.csv",
        "tests/fixtures/hidden.csv",
        "tests/golden/hidden.json",
    ):
        assert path in str(excinfo.value)


def test_clean_filter_physical_difference_rejected(tmp_path):
    """Round-8 boundary note, addressed: a .gitattributes clean filter can
    normalize tampered working bytes back to the expected blob, so default
    (filtering) hash-object would see 'equal' where the pipeline reads
    physically different bytes. --no-filters pins the invariant to raw
    physical bytes and must reject the tampered file."""
    repo = _init_repo(tmp_path / "repo")
    _git("config", "filter.strip.clean", "tr -d X", cwd=repo)
    _write(repo, ".gitattributes", "tests/golden/*.json filter=strip\n")
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", '{"v": 1}\n')
    _commit_all(repo, "golden on main, filter active")
    _git("checkout", "-q", "-b", "feature", cwd=repo)
    # Physically different working bytes; the clean filter strips the X, so a
    # FILTERING hash matches the committed blob and would hide the tampering.
    (repo / GOLDEN_DIR / "vX_scores.json").write_text('{"vX": 1}\n')
    base = resolve_merge_base(repo)
    filtered_hash = _git("hash-object", f"{GOLDEN_DIR}/vX_scores.json", cwd=repo)
    expected_blob = _tree_entry(base, f"{GOLDEN_DIR}/vX_scores.json", repo)[1]
    assert filtered_hash == expected_blob  # the attack premise is real
    with pytest.raises(ReferenceArtifactViolation, match="working tree differs"):
        check_merge_base_goldens(base, repo)


def test_merged_side_branch_created_then_deleted_golden_is_rejected(tmp_path):
    """Round-9 SEVERE: default history simplification follows only one
    TREESAME parent at each merge, so a golden created and deleted on a merged
    side branch left no trace in any path-limited query — the round-8 exploit
    transported through an ordinary --no-ff merge. --full-history must walk
    the side branch and refuse."""
    repo = _init_repo(tmp_path / "repo")
    _write(repo, "README.md", "base\n")
    _commit_all(repo, "base")
    _git("checkout", "-q", "-b", "feature", cwd=repo)
    _write(repo, "feature.txt", "work\n")
    _commit_all(repo, "feature work — first parent never has the golden")
    _git("checkout", "-q", "-b", "side", cwd=repo)
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", '{"v": "transient"}\n')
    _commit_all(repo, "add golden on side")
    (repo / GOLDEN_DIR / "vX_scores.json").unlink()
    _commit_all(repo, "delete golden on side")
    _git("checkout", "-q", "feature", cwd=repo)
    _git("merge", "-q", "--no-edit", "--no-ff", "side", cwd=repo)
    base = resolve_merge_base(repo)
    # The attack premise is real: WITHOUT --full-history the simplified
    # path-history of merge_base..HEAD is empty — the side branch is invisible.
    simplified = _git(
        "log",
        "--format=",
        "--name-only",
        "-z",
        "--no-renames",
        f"{base}..HEAD",
        "--",
        GOLDEN_DIR,
        cwd=repo,
    )
    assert not simplified.strip("\0")
    with pytest.raises(ReferenceArtifactViolation, match="survive to the tip"):
        check_created_goldens(base, repo)


def test_prior_version_hidden_in_merged_ancestry_is_not_genuinely_new(tmp_path):
    """Round-9, second shape: a golden added and deleted on a side branch
    BEFORE the merge base, merged into main, leaves a prior version reachable
    from the merge base that simplified history never visits — reintroducing
    the path inside a change would wrongly qualify as genuinely new. The
    --full-history prior-version lookup must refuse."""
    repo = _init_repo(tmp_path / "repo")
    _write(repo, "README.md", "base\n")
    _commit_all(repo, "base")
    _git("checkout", "-q", "-b", "side", cwd=repo)
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", '{"v": "old"}\n')
    _commit_all(repo, "golden once existed on side")
    (repo / GOLDEN_DIR / "vX_scores.json").unlink()
    _commit_all(repo, "golden removed on side")
    _git("checkout", "-q", "main", cwd=repo)
    _git("merge", "-q", "--no-edit", "--no-ff", "side", cwd=repo)
    _git("checkout", "-q", "-b", "feature", cwd=repo)
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", '{"v": "laundered"}\n')
    _commit_all(repo, "reintroduce with different bytes inside the change")
    base = resolve_merge_base(repo)
    with pytest.raises(ReferenceArtifactViolation, match="not genuinely new"):
        check_created_goldens(base, repo)


def test_committed_head_seed_tamper_rejected_despite_safe_index_and_worktree(
    tmp_path,
):
    """Round-10 SEVERE, seed shape: commit malicious seed bytes at HEAD, then
    restore the safe bytes to BOTH the index and the working tree without
    committing. The working-tree and index legs compare against the merge
    base and are clean — but a merge of HEAD carries the malicious COMMITTED
    bytes. The committed-HEAD leg must fail (mirror image of round 7)."""
    repo = _init_repo(tmp_path / "repo")
    safe = "player_id,name\n1,safe\n"
    _write(repo, "data/seed/players.csv", safe)
    _commit_all(repo, "seed on main")
    _git("checkout", "-q", "-b", "feature", cwd=repo)
    _write(repo, "data/seed/players.csv", "player_id,name\n1,malicious\n")
    _commit_all(repo, "malicious bytes COMMITTED at HEAD")
    _write(repo, "data/seed/players.csv", safe)
    _git("add", "data/seed/players.csv", cwd=repo)  # index AND worktree safe
    base = resolve_merge_base(repo)
    # The attack premise is real: both pre-round-10 legs are clean.
    assert not _git("diff", base, "--stat", "--", "data/seed", cwd=repo)
    assert not _git("diff", "--cached", base, "--stat", "--", "data/seed", cwd=repo)
    with pytest.raises(ReferenceArtifactViolation, match="committed HEAD"):
        check_seed_and_fixtures(base, repo)


def test_committed_head_golden_tamper_rejected_despite_safe_index_and_worktree(
    tmp_path,
):
    """Round-10 SEVERE, golden shape: commit a tampered golden at HEAD, then
    restore the pinned bytes to the index and working tree. The staged-entry
    and working-bytes checks pass against the merge-base entry — the
    committed HEAD tree entry must fail."""
    repo = _init_repo(tmp_path / "repo")
    pinned = '{"goat_score": 57.58}\n'
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", pinned)
    _commit_all(repo, "golden on main")
    _git("checkout", "-q", "-b", "feature", cwd=repo)
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", '{"goat_score": 99.99}\n')
    _commit_all(repo, "tampered golden COMMITTED at HEAD")
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", pinned)
    _git("add", f"{GOLDEN_DIR}/vX_scores.json", cwd=repo)  # both legs safe
    base = resolve_merge_base(repo)
    # The attack premise is real: staged entry and working bytes both equal
    # the merge-base entry — only the committed HEAD tree differs.
    assert _index_entry(f"{GOLDEN_DIR}/vX_scores.json", repo) == _tree_entry(
        base, f"{GOLDEN_DIR}/vX_scores.json", repo
    )
    with pytest.raises(ReferenceArtifactViolation, match="committed HEAD"):
        check_merge_base_goldens(base, repo)


def test_created_golden_modify_then_restore_is_rejected(tmp_path):
    """Round-10 fix 3: 'one add, zero deletes' never inspected MODIFY
    commits, so create -> modify-to-malicious -> restore left the tip, index,
    and working tree all equal to the creation entry while an intermediate
    commit carried different bytes — contradicting 'pinned from creation
    through the tip'. The ancestry-path walk must refuse."""
    repo = _init_repo(tmp_path / "repo")
    _write(repo, "README.md", "base\n")
    _commit_all(repo, "base")
    _git("checkout", "-q", "-b", "feature", cwd=repo)
    v1 = '{"v": 1}\n'
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", v1)
    _commit_all(repo, "creation")
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", '{"v": 666}\n')
    _commit_all(repo, "modify to malicious mid-branch")
    _write(repo, f"{GOLDEN_DIR}/vX_scores.json", v1)
    _commit_all(repo, "restore creation bytes before the tip")
    base = resolve_merge_base(repo)
    with pytest.raises(ReferenceArtifactViolation, match="EVERY commit"):
        check_created_goldens(base, repo)

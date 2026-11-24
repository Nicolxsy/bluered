"""AST-based Stable Dependencies Principle (SDP) mathematical auditor."""

from __future__ import annotations

import ast
import sys
from dataclasses import dataclass
from pathlib import Path

TARGET_PACKAGES = (
    ("client", "bluered_client"),
    ("server", "bluered_server"),
)


@dataclass(frozen=True, slots=True)
class ComponentMetrics:
    name: str
    ca: int
    ce: int
    instability: float


@dataclass(frozen=True, slots=True)
class AuditResult:
    metrics: dict[str, ComponentMetrics]
    violations: list[str]

    @property
    def ok(self) -> bool:
        return len(self.violations) == 0


def _module_from_file(pkg_dir: Path, pkg_name: str, file_path: Path) -> str:
    rel = file_path.relative_to(pkg_dir).with_suffix("")
    parts = list(rel.parts)
    if parts and parts[-1] == "__init__":
        parts.pop()
    return ".".join([pkg_name, *parts]) if parts else pkg_name


def _component_of(module_name: str) -> str | None:
    parts = module_name.split(".")
    if parts[0] not in {"bluered_client", "bluered_server"}:
        return None
    if len(parts) == 1:
        return parts[0]
    return f"{parts[0]}.{parts[1]}"


def _resolve_relative_import(
    current_module: str, is_pkg_init: bool, level: int, mod: str | None
) -> str:
    parts = current_module.split(".")
    base = parts if is_pkg_init else parts[:-1]
    trim = level - 1
    anchor = base[: len(base) - trim] if trim > 0 else base
    return ".".join([*anchor, mod]) if mod else ".".join(anchor)


def _extract_imports(file_path: Path, current_module: str) -> set[str]:
    tree = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
    is_pkg_init = file_path.name == "__init__.py"
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level > 0:
                resolved = _resolve_relative_import(
                    current_module, is_pkg_init, node.level, node.module
                )
                if resolved:
                    imports.add(resolved)
            elif node.module:
                imports.add(node.module)
    return imports


def audit_workspace(root: Path) -> AuditResult:
    """Compute Ca, Ce, and Instability I = Ce / (Ca + Ce) across client/ and server/."""
    components: set[str] = set()
    internal_edges: set[tuple[str, str]] = set()
    violations: list[str] = []

    for sub_dir, pkg_name in TARGET_PACKAGES:
        pkg_dir = root / sub_dir / pkg_name
        if not pkg_dir.is_dir():
            continue
        for py_file in sorted(pkg_dir.rglob("*.py")):
            mod_name = _module_from_file(pkg_dir, pkg_name, py_file)
            src_comp = _component_of(mod_name)
            if src_comp is None or src_comp == pkg_name:
                continue
            components.add(src_comp)

            for imported in _extract_imports(py_file, mod_name):
                top_level = imported.split(".")[0]
                if (
                    src_comp.endswith(".domain")
                    and top_level not in sys.stdlib_module_names
                    and imported != src_comp
                    and not imported.startswith(f"{src_comp}.")
                ):
                    violations.append(
                        f"Domain Core violation: {mod_name} imports non-stdlib '{imported}'"
                    )

                dst_comp = _component_of(imported)
                if dst_comp and dst_comp != pkg_name and dst_comp != src_comp:
                    components.add(dst_comp)
                    internal_edges.add((src_comp, dst_comp))

    metrics: dict[str, ComponentMetrics] = {}
    for comp in sorted(components):
        ca = sum(1 for _, dst in internal_edges if dst == comp)
        ce = sum(1 for src, _ in internal_edges if src == comp)
        total = ca + ce
        instability = (ce / total) if total > 0 else 0.0
        metrics[comp] = ComponentMetrics(name=comp, ca=ca, ce=ce, instability=instability)
        if comp.endswith(".domain") and instability > 0.0:
            violations.append(
                f"Domain Core instability violation: {comp} has I={instability:.2f} (must be 0.00)"
            )

    for src, dst in sorted(internal_edges):
        i_src = metrics[src].instability
        i_dst = metrics[dst].instability
        if i_dst > i_src:
            violations.append(
                f"SDP violation: {src} (I={i_src:.2f}) -> {dst} (I={i_dst:.2f}); "
                f"dependencies must flow toward lower instability"
            )

    return AuditResult(metrics=metrics, violations=violations)


def main() -> int:
    result = audit_workspace(Path.cwd())
    print("Component                       Ca   Ce   Instability (I)")
    print("-" * 57)
    for m in result.metrics.values():
        print(f"{m.name:<31} {m.ca:>2}   {m.ce:>2}   {m.instability:>15.2f}")
    if result.violations:
        print("\nViolations:")
        for v in result.violations:
            print(f"  - {v}")
        return 1
    print("\nSDP audit passed: 0 violations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

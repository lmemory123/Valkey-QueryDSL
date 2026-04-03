#!/usr/bin/env python3
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
POMS = sorted(ROOT.rglob("pom.xml"))
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
ET.register_namespace("", NS["m"])

PROJECT_ARTIFACT_IDS = {
    "valkey-querydsl",
    "valkey-query-annotations",
    "valkey-query-core",
    "valkey-query-processor",
    "valkey-query-glide-adapter",
    "valkey-query-spring-boot-starter",
    "valkey-query-test-example",
}
KNOWN_PROJECT_GROUP_IDS = {"com.momao", "io.github.lmemory123"}


def text(elem):
    return elem.text.strip() if elem is not None and elem.text else ""


def sibling(parent, tag_name):
    for child in list(parent):
        if child.tag == f"{{{NS['m']}}}{tag_name}":
            return child
    return None


def update_pom(pom_path: Path, group_id: str) -> bool:
    tree = ET.parse(pom_path)
    root = tree.getroot()
    changed = False
    parent_map = {child: parent for parent in tree.iter() for child in parent}

    for group_elem in root.findall(".//m:groupId", NS):
        parent = parent_map.get(group_elem)
        if parent is None:
            continue
        artifact_elem = sibling(parent, "artifactId")
        if artifact_elem is None:
            continue
        artifact_id = text(artifact_elem)
        if artifact_id not in PROJECT_ARTIFACT_IDS:
            continue
        if text(group_elem) not in KNOWN_PROJECT_GROUP_IDS:
            continue
        if text(group_elem) == group_id:
            continue
        group_elem.text = group_id
        changed = True

    if changed:
        tree.write(pom_path, encoding="utf-8", xml_declaration=False)
    return changed


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: set-project-group-id.py <group-id>", file=sys.stderr)
        return 1

    group_id = sys.argv[1].strip()
    if not group_id:
        print("GroupId must not be empty.", file=sys.stderr)
        return 1

    changed_files = [str(p.relative_to(ROOT)) for p in POMS if update_pom(p, group_id)]
    print(f"Updated project groupId to {group_id}")
    if changed_files:
        for file in changed_files:
            print(f" - {file}")
    else:
        print(" - no pom.xml changes were required")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
POMS = sorted(ROOT.rglob("pom.xml"))
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
ET.register_namespace("", NS["m"])
KNOWN_PROJECT_GROUP_IDS = {"com.momao", "io.github.lmemory123"}


def text(elem):
    return elem.text.strip() if elem is not None and elem.text else ""


def update_pom(pom_path: Path, version: str) -> bool:
    tree = ET.parse(pom_path)
    root = tree.getroot()
    changed = False

    artifact_id = root.find("m:artifactId", NS)
    if text(artifact_id) == "valkey-querydsl":
        version_elem = root.find("m:version", NS)
        if version_elem is not None and text(version_elem) != version:
            version_elem.text = version
            changed = True

    parent = root.find("m:parent", NS)
    if parent is not None:
        parent_group = parent.find("m:groupId", NS)
        parent_artifact = parent.find("m:artifactId", NS)
        parent_version = parent.find("m:version", NS)
        if (
            text(parent_group) in KNOWN_PROJECT_GROUP_IDS
            and text(parent_artifact) == "valkey-querydsl"
            and parent_version is not None
            and text(parent_version) != version
        ):
            parent_version.text = version
            changed = True

    if changed:
        tree.write(pom_path, encoding="utf-8", xml_declaration=False)
    return changed


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: set-project-version.py <version>", file=sys.stderr)
        return 1

    version = sys.argv[1].strip()
    if not version:
        print("Version must not be empty.", file=sys.stderr)
        return 1

    changed_files = [str(p.relative_to(ROOT)) for p in POMS if update_pom(p, version)]
    print(f"Updated project version to {version}")
    if changed_files:
        for file in changed_files:
            print(f" - {file}")
    else:
        print(" - no pom.xml changes were required")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

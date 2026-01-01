# xml2duck.py  (library; no CLI)
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union

import yaml
from lxml import etree
import dlt

from utils import sanitize_table_name


# -----------------------------
# XML helpers
# -----------------------------
def strip_ns(tag: str) -> str:
    if tag.startswith("{") and "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def prune_elem(elem: etree._Element) -> None:
    parent = elem.getparent()
    elem.clear()
    while parent is not None and elem.getprevious() is not None:
        try:
            del parent[0]
        except Exception:
            break


def iterparse_root_tag(xml_path: str) -> str:
    for _event, elem in etree.iterparse(xml_path, events=("start",), recover=True, huge_tree=True):
        return strip_ns(elem.tag)
    raise ValueError("XML is empty or unreadable.")


def elem_to_lossless_obj(elem: etree._Element) -> Dict[str, Any]:
    obj: Dict[str, Any] = {}
    if elem.attrib:
        obj["@"] = {strip_ns(k): v for k, v in elem.attrib.items()}

    children = list(elem)
    if children:
        grouped: Dict[str, List[Any]] = defaultdict(list)
        for ch in children:
            grouped[strip_ns(ch.tag)].append(elem_to_lossless_obj(ch))
        for tag, items in grouped.items():
            obj[tag] = items if len(items) > 1 else items[0]

    text = (elem.text or "").strip()
    if text:
        obj["#text"] = text
    return obj


def stable_hash_obj(obj: Dict[str, Any]) -> str:
    data = yaml.safe_dump(obj, sort_keys=True, allow_unicode=True).encode("utf-8")
    return hashlib.sha1(data).hexdigest()


# -----------------------------
# Mapping model (XML)
# -----------------------------
@dataclass
class PKRule:
    prefer: List[str]


@dataclass
class ReferenceRule:
    name: str
    from_fields: List[str]
    to_collection: str
    to_pk: str
    cardinality: str
    confidence: float = 0.0
    reason: str = ""


@dataclass
class CollectionRule:
    path: str
    pk: PKRule
    keep_all_fields: bool = True
    references: List[ReferenceRule] = field(default_factory=list)
    parent: Optional[str] = None
    parent_fk: Optional[str] = None
    enabled: bool = True


@dataclass
class XmlMapping:
    version: int
    root: str
    collections: Dict[str, CollectionRule]


# -----------------------------
# Semi-flat row writer (XML)
# -----------------------------
def to_semiflat_row_xml(obj: Dict[str, Any], pk: str, add_raw_json: bool = True, include_attrs: bool = True) -> Dict[str, Any]:
    row: Dict[str, Any] = {"_pk": pk}
    if add_raw_json:
        row["raw_json"] = json.dumps(obj, ensure_ascii=False)

    if include_attrs:
        attrs = obj.get("@")
        if isinstance(attrs, dict):
            for k, v in attrs.items():
                row[f"attr__{k}"] = v

    if "#text" in obj and isinstance(obj["#text"], (str, int, float)):
        row["text"] = obj["#text"]

    for k, v in obj.items():
        if k in ("@", "#text"):
            continue

        if isinstance(v, (str, int, float, bool)) or v is None:
            row[k] = v
        elif isinstance(v, dict):
            keys = set(v.keys())
            if "#text" in v and keys.issubset({"@", "#text"}):
                row[k] = v["#text"]
                if include_attrs and "@" in v and isinstance(v["@"], dict):
                    row[f"json__{k}__attrs"] = json.dumps(v["@"], ensure_ascii=False)
            else:
                row[f"json__{k}"] = json.dumps(v, ensure_ascii=False)
        else:
            row[f"json__{k}"] = json.dumps(v, ensure_ascii=False)

    return row


# -----------------------------
# Field extraction (refs/pk inference)
# -----------------------------
REFISH_RE = re.compile(r"(ref|refs|_id|id|code)$", re.IGNORECASE)
CODE_RE = re.compile(r"(code)$", re.IGNORECASE)
PK_BAD_RE = re.compile(r"(changecode|changetype|status|type|version|flag)$", re.IGNORECASE)


def extract_scalar_fields_xml(obj: Dict[str, Any]) -> Dict[str, Union[str, List[str]]]:
    out: Dict[str, Union[str, List[str]]] = {}
    attrs = obj.get("@")
    if isinstance(attrs, dict):
        for k, v in attrs.items():
            kk = f"@{k}"
            if isinstance(v, (str, int, float)) and str(v).strip():
                out[kk] = str(v).strip()

    for k, v in obj.items():
        if k in ("@", "#text"):
            continue
        if isinstance(v, (str, int, float)) and str(v).strip():
            out[k] = str(v).strip()
        elif isinstance(v, dict):
            if "#text" in v and isinstance(v["#text"], (str, int, float)) and str(v["#text"]).strip():
                out[k] = str(v["#text"]).strip()
        elif isinstance(v, list):
            vals: List[str] = []
            for item in v:
                if isinstance(item, dict) and "#text" in item and str(item["#text"]).strip():
                    vals.append(str(item["#text"]).strip())
            if vals:
                out[k] = vals
    return out


def _get_candidate_value(obj: Dict[str, Any], key: str) -> Optional[str]:
    if key.startswith("@"):
        attrs = obj.get("@")
        if isinstance(attrs, dict):
            v = attrs.get(key[1:])
            if v is None:
                return None
            s = str(v).strip()
            return s if s else None
        return None

    v = obj.get(key)
    if isinstance(v, (str, int, float)):
        s = str(v).strip()
        return s if s else None
    if isinstance(v, dict) and "#text" in v:
        s = str(v["#text"]).strip()
        return s if s else None
    return None


def infer_pk_prefer_list(observed_attr_keys: Counter[str], observed_scalar_keys: Counter[str]) -> List[str]:
    base = ["@id", "id", "@ID", "ID", "@code", "code", "@Code", "Code", "@key", "key"]
    obs = []
    for k, _ in observed_attr_keys.most_common(50):
        kk = f"@{k}"
        if kk not in obs:
            obs.append(kk)
    for k, _ in observed_scalar_keys.most_common(50):
        if k.lower() in ("id", "code", "key") and k not in obs:
            obs.append(k)

    merged = []
    for x in obs + base:
        if x not in merged:
            merged.append(x)
    return merged


def reorder_pk_prefer_by_uniqueness(prefer: List[str], obj_iter: Iterable[Dict[str, Any]], distinct_limit: int = 2_000_000) -> List[str]:
    total = 0
    nonnull = Counter()
    distinct_sets: Dict[str, set] = {k: set() for k in prefer}

    for obj in obj_iter:
        total += 1
        for k in prefer:
            v = _get_candidate_value(obj, k)
            if v is None:
                continue
            nonnull[k] += 1
            s = distinct_sets[k]
            if len(s) < distinct_limit:
                s.add(v)
        if total >= 500_000:
            break

    def score(k: str) -> float:
        nn = nonnull.get(k, 0)
        if total == 0:
            return -1.0
        null_ratio = 1.0 - (nn / total)
        distinct = len(distinct_sets[k])
        distinct_rate = (distinct / nn) if nn else 0.0
        penalty = 0.0
        if PK_BAD_RE.search(k):
            penalty += 0.50
        return distinct_rate - 0.25 * null_ratio - penalty

    ranked = sorted([(score(k), -i, k) for i, k in enumerate(prefer)], reverse=True)
    return [k for _, __, k in ranked]


def get_pk_from_obj(obj: Dict[str, Any], pk_rule: PKRule) -> Optional[str]:
    for key in pk_rule.prefer:
        if key.startswith("@"):
            attr = key[1:]
            attrs = obj.get("@")
            if isinstance(attrs, dict) and attr in attrs and str(attrs[attr]).strip():
                return str(attrs[attr]).strip()
        else:
            v = obj.get(key)
            if isinstance(v, (str, int, float)) and str(v).strip():
                return str(v).strip()
            if isinstance(v, dict) and "#text" in v and str(v["#text"]).strip():
                return str(v["#text"]).strip()
    return None


# -----------------------------
# Path detection + entity iterators
# -----------------------------
def iter_paths_counts(xml_path: str, max_depth: int = 6) -> Counter[Tuple[str, ...]]:
    counts: Counter[Tuple[str, ...]] = Counter()
    stack: List[str] = []
    context = etree.iterparse(xml_path, events=("start", "end"), recover=True, huge_tree=True)
    for event, elem in context:
        if event == "start":
            stack.append(strip_ns(elem.tag))
        else:
            if len(stack) <= max_depth:
                counts[tuple(stack)] += 1
            stack.pop()
            prune_elem(elem)
    del context
    return counts


def infer_root_singletons(root: str, counts: Counter[Tuple[str, ...]]) -> Dict[str, CollectionRule]:
    singletons: Dict[str, CollectionRule] = {}
    child_counts = Counter()
    for path, c in counts.items():
        if len(path) == 2 and path[0] == root:
            child_counts[path[1]] += c
    for child, c in child_counts.items():
        if c == 1:
            singletons[child] = CollectionRule(path=f"/{root}/{child}", pk=PKRule(prefer=["@id", "id", "Code", "code", "@key", "key"]))
    return singletons


def infer_collections_from_counts(root: str, counts: Counter[Tuple[str, ...]], min_repeats: int = 2) -> Dict[str, CollectionRule]:
    collections: Dict[str, CollectionRule] = {}

    # /root/container/entity
    top_ctr: Dict[Tuple[str, str], Counter[str]] = defaultdict(Counter)
    for path, c in counts.items():
        if len(path) == 3 and path[0] == root:
            container = path[1]
            entity = path[2]
            top_ctr[(root, container)][entity] += c

    for (_r, container), ctr in top_ctr.items():
        entity, cnt = ctr.most_common(1)[0]
        if cnt >= min_repeats:
            collections[container] = CollectionRule(
                path=f"/{root}/{container}/{entity}",
                pk=PKRule(prefer=["@id", "id", "@code", "code", "@key", "key"]),
            )

    # /root/container/entity/child_container/child_entity
    nested_ctr: Dict[Tuple[str, str, str], Counter[str]] = defaultdict(Counter)
    for path, c in counts.items():
        if len(path) == 5 and path[0] == root:
            _root, container, entity, child_container, child_entity = path
            nested_ctr[(container, entity, child_container)][child_entity] += c

    for (container, _entity, child_container), ctr in nested_ctr.items():
        child_entity, cnt = ctr.most_common(1)[0]
        if cnt < min_repeats:
            continue

        parent_collection = container if container in collections else container
        cname_candidate = child_container
        if cname_candidate in collections:
            cname_candidate = f"{parent_collection}_{child_container}"

        collections[cname_candidate] = CollectionRule(
            path=f"/{root}/{container}/{_entity}/{child_container}/{child_entity}",
            pk=PKRule(prefer=["@id", "id", "@code", "code", "@key", "key"]),
            parent=parent_collection,
            parent_fk="_parent_pk",
        )

    return collections


def parse_path(path: str) -> List[str]:
    parts = [p for p in path.strip("/").split("/") if p]
    if len(parts) not in (2, 3, 5):
        raise ValueError(f"Supported paths are /A/B or /A/B/C or /A/B/C/D/E. Got: {path}")
    return parts


def tuple_ci(xs: List[str]) -> Tuple[str, ...]:
    return tuple((x or "").lower() for x in xs)


def get_ci(d: Dict[str, Any], key: str) -> Any:
    if key in d:
        return d[key]
    kl = key.lower()
    for k, v in d.items():
        if isinstance(k, str) and k.lower() == kl:
            return v
    return None


def iter_entities_by_path(xml_path: str, abs_path: str, root: str) -> Iterator[Dict[str, Any]]:
    parts = parse_path(abs_path)
    if parts[0].lower() != root.lower():
        return

    target = tuple_ci(parts)
    stack: List[str] = []
    context = etree.iterparse(xml_path, events=("start", "end"), recover=True, huge_tree=True)

    for event, elem in context:
        if event == "start":
            stack.append(strip_ns(elem.tag))
            continue

        stack_ci = tuple_ci(stack)

        if stack_ci == target:
            yield elem_to_lossless_obj(elem)
            stack.pop()
            prune_elem(elem)
            continue

        if len(stack_ci) > len(target) and stack_ci[: len(target)] == target:
            stack.pop()
            continue

        stack.pop()
        prune_elem(elem)

    del context


def iter_nested_entities_with_parent(xml_path: str, abs_child_path: str, root: str, parent_abs_path: str) -> Iterator[Tuple[Dict[str, Any], Dict[str, Any]]]:
    p_parts = parse_path(parent_abs_path)
    c_parts = parse_path(abs_child_path)
    if len(p_parts) != 3 or len(c_parts) != 5:
        raise ValueError("Expected parent path len=3 and child path len=5.")
    if tuple_ci(p_parts[:3]) != tuple_ci(c_parts[:3]):
        raise ValueError("Child path must start with parent path.")

    child_container = c_parts[3]
    child_entity = c_parts[4]

    target_parent = tuple_ci(p_parts)
    stack: List[str] = []
    context = etree.iterparse(xml_path, events=("start", "end"), recover=True, huge_tree=True)

    for event, elem in context:
        if event == "start":
            stack.append(strip_ns(elem.tag))
            continue

        stack_ci = tuple_ci(stack)

        if stack_ci == target_parent:
            parent_obj = elem_to_lossless_obj(elem)
            cc = get_ci(parent_obj, child_container)

            def yield_children(container_obj: Any) -> Iterator[Dict[str, Any]]:
                if isinstance(container_obj, dict):
                    ce = get_ci(container_obj, child_entity)
                    if isinstance(ce, list):
                        for x in ce:
                            if isinstance(x, dict):
                                yield x
                    elif isinstance(ce, dict):
                        yield ce
                elif isinstance(container_obj, list):
                    for item in container_obj:
                        if isinstance(item, dict):
                            ce = get_ci(item, child_entity)
                            if isinstance(ce, list):
                                for x in ce:
                                    if isinstance(x, dict):
                                        yield x
                            elif isinstance(ce, dict):
                                yield ce

            for child_obj in yield_children(cc):
                yield child_obj, parent_obj

            stack.pop()
            prune_elem(elem)
            continue

        if len(stack_ci) > len(target_parent) and stack_ci[: len(target_parent)] == target_parent:
            stack.pop()
            continue

        stack.pop()
        prune_elem(elem)

    del context


# -----------------------------
# Public: infer XML mapping into a generic YAML structure
# -----------------------------
def infer_xml_mapping(xml_path: str) -> XmlMapping:
    root = iterparse_root_tag(xml_path)
    counts = iter_paths_counts(xml_path, max_depth=6)
    collections = infer_collections_from_counts(root, counts, min_repeats=2)

    singletons = infer_root_singletons(root, counts)
    for k, v in singletons.items():
        name = k if k not in collections else f"Meta_{k}"
        collections[name] = v

    if not collections:
        raise ValueError("Could not infer XML collections (no repeating entity paths).")

    # improve pk prefer per collection (quick sample + scan)
    for cname, rule in collections.items():
        parts = parse_path(rule.path)

        def obj_stream() -> Iterator[Dict[str, Any]]:
            if len(parts) in (2, 3):
                yield from iter_entities_by_path(xml_path, rule.path, root)
            else:
                parent_abs = "/" + "/".join(parts[:3])
                for child_obj, _parent_obj in iter_nested_entities_with_parent(xml_path, rule.path, root, parent_abs):
                    yield child_obj

        observed_attr_keys = Counter()
        observed_scalar_keys = Counter()
        for i, obj in enumerate(obj_stream(), 1):
            attrs = obj.get("@")
            if isinstance(attrs, dict):
                for k in attrs.keys():
                    observed_attr_keys[k] += 1
            scalars = extract_scalar_fields_xml(obj)
            for k in scalars.keys():
                if not k.startswith("@"):
                    observed_scalar_keys[k] += 1
            if i >= 3000:
                break

        rule.pk.prefer = infer_pk_prefer_list(observed_attr_keys, observed_scalar_keys)
        rule.pk.prefer = reorder_pk_prefer_by_uniqueness(rule.pk.prefer, obj_stream())

    return XmlMapping(version=1, root=root, collections=collections)


# -----------------------------
# Public: build dlt resources (raw + normalized)
# -----------------------------
def build_xml_resources(
    xml_path: str,
    xml_mapping: XmlMapping,
    raw_table: str,
    write_disposition: str = "append",
):
    root = xml_mapping.root

    raw_table = sanitize_table_name(raw_table, "raw_ingest")

    @dlt.resource(name=raw_table, write_disposition=write_disposition)
    def raw_resource():
        for cname, rule in xml_mapping.collections.items():
            if not getattr(rule, "enabled", True):
                continue
            parts = parse_path(rule.path)
            if len(parts) in (2, 3):
                for obj in iter_entities_by_path(xml_path, rule.path, root):
                    yield {
                        "collection": cname,
                        "path": rule.path,
                        "raw_json": json.dumps(obj, ensure_ascii=False),
                    }
            else:
                parent_abs = "/" + "/".join(parts[:3])
                for child_obj, parent_obj in iter_nested_entities_with_parent(xml_path, rule.path, root, parent_abs):
                    yield {
                        "collection": cname,
                        "path": rule.path,
                        "raw_json": json.dumps(child_obj, ensure_ascii=False),
                        "_parent_raw_json": json.dumps(parent_obj, ensure_ascii=False),
                        "_parent_path": parent_abs,
                    }

    normalized_resources: Dict[str, Any] = {}

    def mk_norm_resource(cname: str, rule: CollectionRule):
        tname = sanitize_table_name(cname, "xml_data")

        @dlt.resource(name=tname, write_disposition=write_disposition)
        def _res():
            parts = parse_path(rule.path)
            if len(parts) in (2, 3):
                for obj in iter_entities_by_path(xml_path, rule.path, root):
                    pk = get_pk_from_obj(obj, rule.pk) or stable_hash_obj(obj)
                    yield to_semiflat_row_xml(obj, str(pk), add_raw_json=True, include_attrs=True)
            else:
                parent_abs = "/" + "/".join(parts[:3])
                # find parent rule
                parent_collection = rule.parent
                parent_rule = xml_mapping.collections.get(parent_collection) if parent_collection else None
                if not parent_rule:
                    for pc, pr in xml_mapping.collections.items():
                        if pr.path == parent_abs:
                            parent_collection = pc
                            parent_rule = pr
                            break

                for child_obj, parent_obj in iter_nested_entities_with_parent(xml_path, rule.path, root, parent_abs):
                    child_pk = get_pk_from_obj(child_obj, rule.pk) or stable_hash_obj(child_obj)
                    row = to_semiflat_row_xml(child_obj, str(child_pk), add_raw_json=True, include_attrs=True)

                    parent_pk = None
                    if parent_rule:
                        parent_pk = get_pk_from_obj(parent_obj, parent_rule.pk) or stable_hash_obj(parent_obj)

                    row[rule.parent_fk or "_parent_pk"] = str(parent_pk) if parent_pk is not None else None
                    row["_parent_collection"] = parent_collection
                    yield row

        return _res

    for cname, rule in xml_mapping.collections.items():
        if not getattr(rule, "enabled", True):
            continue
        normalized_resources[cname] = mk_norm_resource(cname, rule)

    return raw_resource, normalized_resources

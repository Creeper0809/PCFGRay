from pathlib import Path
from typing import Any, Dict, List, Set
from itertools import count

from graphviz import Digraph
from pcfg_lib.guess.pcfg.pcfg_guesser import PCFGGuesser  # type: ignore

class Node:
    _id_seq = count(0)

    def __init__(self, label: str):
        self.id = f"n{next(Node._id_seq)}"
        self.label = label
        self.children: Dict[str, "Node"] = {}
        self.edge_cnt: Dict[str, int] = {}

    def add_path(self, symbols: List[str], weight: int = 1) -> None:
        if not symbols:
            return
        first, *rest = symbols
        if first not in self.children:
            self.children[first] = Node(first)
            self.edge_cnt[first] = 0
        self.edge_cnt[first] += weight
        self.children[first].add_path(rest, weight)

    def dfs(
        self,
        g: Digraph,
        *,
        highlight: Set[str],
        depth: int,
        max_depth: int | None,
        topk: int | None,
    ) -> None:
        # 노드 그리기
        attrs = {"shape": "circle"}
        if self.label in highlight:
            attrs.update({
                "style": "filled",
                "fillcolor": "#FFD37F",
                "penwidth": "2",
            })
        g.node(self.id, self.label, **attrs)

        # 깊이 제한에 걸리면 … 표시하고 하위 트리 중단
        if max_depth is not None and depth >= max_depth:
            if self.children:
                cut = f"cut{self.id}"
                g.node(cut, label="…", shape="plaintext")
                g.edge(self.id, cut, style="dotted")
            return

        # 자식 노드들 중 상위 확률 순으로 정렬
        items = sorted(
            self.children.items(),
            key=lambda kv: self.edge_cnt[kv[0]],
            reverse=True,
        )
        if topk is not None:
            items = items[:topk]

        total = sum(self.edge_cnt[lbl] for lbl, _ in items) or 1
        for lbl, child in items:
            prob = self.edge_cnt[lbl] / total
            edge_attrs = {"label": f"{prob:.2f}"}
            if self.label in highlight or lbl in highlight:
                edge_attrs.update({"color": "#FF8800", "penwidth": "2"})
            g.edge(self.id, child.id, **edge_attrs)
            child.dfs(
                g,
                highlight=highlight,
                depth=depth + 1,
                max_depth=max_depth,
                topk=topk,
            )

###############################################################################
# 시퀀스 추출 & 트리 빌드
###############################################################################

def _extract_sequences(raw: Any) -> List[List[str]]:
    """
    PCFGGuesser.base_structure 혹은 config['seqs'] 형식의
    raw 데이터를 받아서 ["A","B",...] 형태의 시퀀스 리스트로 변환.
    """
    seqs: List[List[str]] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                for k, v in item.items():
                    if isinstance(v, list) and "REPLACEMENTS" in str(k).upper():
                        seqs.append(v)
                        break
            elif isinstance(item, list):
                seqs.append(item)
    elif isinstance(raw, dict):
        # 단일 dict도 재귀 처리
        seqs.extend(_extract_sequences([raw]))
    else:
        raise ValueError(f"Unsupported data type: {type(raw)}")
    return seqs

def _build_tree(seqs: List[List[str]]) -> Node:
    """
    리스트 형태 시퀀스(seq of token-lists)를 받아
    첫 글자(prefix)만 따낸 뒤 트리로 만듦.
    """
    root = Node("S")
    for seq in seqs:
        # 각 토큰의 첫 글자만 사용
        root.add_path([tok[0] if tok[0] != "S" else "$" if tok else "" for tok in seq])
    return root

###############################################################################
# Public API: grammar_tree
###############################################################################

def grammar_tree(config: Dict[str, Any]) -> Path:
    """
    config:
      - seqs         : choice, 없으면 PCFGGuesser({}).base_structure 사용
      - highlight    : 강조할 문자 리스트 (예: ["H"])
      - max_depth    : 트리 깊이 제한 (None이면 무제한)
      - topk         : 부모당 최대 자식 수 (None이면 전체)
      - outfile, fmt, rankdir : Graphviz 옵션
    """
    # 1) 원본 시퀀스 확보
    if "seqs" in config:
        raw = config["seqs"]
    else:
        raw = PCFGGuesser({}).base_structure  # type: ignore
    seqs = _extract_sequences(raw)
    if not seqs:
        raise ValueError("REPLACEMENTS 시퀀스를 찾지 못했습니다.")

    # 2) 옵션 파싱
    highlight: Set[str] = set(config.get("highlight", []))
    max_depth = config.get("max_depth")
    topk = config.get("topk")
    outfile = config.get("outfile", "grammar_tree")
    fmt = config.get("fmt", "png")
    rankdir = config.get("rankdir", "LR")

    # 3) 트리 생성 & 렌더링
    root = _build_tree(seqs)
    g = Digraph(
        "GrammarTree",
        format=fmt,
        graph_attr={"rankdir": rankdir},
        node_attr={"shape": "circle", "fontname": "Malgun Gothic"},
    )
    root.dfs(
        g,
        highlight=highlight,
        depth=0,
        max_depth=max_depth,
        topk=topk,
    )

    return Path(g.render(outfile, view=True))

def main():
    raw = PCFGGuesser({}).base_structure
    all_seqs = _extract_sequences(raw)


    h_seqs: List[List[str]] = [
        seq for seq in all_seqs
        if any(tok.startswith("H") for tok in seq)
    ]


    wrapped = [{"replacements": seq} for seq in h_seqs]
    outpath = grammar_tree({
        "seqs": wrapped,
        "highlight": ["H"],
        "max_depth": 6,      # 필요에 따라 조절
        "topk": 2,           # 필요에 따라 조절
        "outfile": "h_anywhere_tree",
        "fmt": "png",
        "rankdir": "LR",
    })
    print(f"Rendered graph at: {outpath}")

if __name__ == "__main__":
    main()

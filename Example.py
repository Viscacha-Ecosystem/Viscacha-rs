"""
* the council speaks! * 

100 "expert" agents, each with a unique discipline and stance 
answer your question simultaneously through the Rust job server.
A synthesis agent reads all 100 and creates the final answer.

"""

import os
import sys
import textwrap
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import anthropic
from viscacha import Client, Worker, GUIDashboard
import time


SERVER  = "http://localhost:8000"
MODEL   = "claude-haiku-4-5-20251001"
WORKERS = 20   #  worker threads

DISCIPLINES = [
    "physics",          "evolutionary biology",  "economics",
    "ancient history",  "philosophy",            "mechanical engineering",
    "psychology",       "sociology",             "pure mathematics",
    "computer science", "medicine",              "constitutional law",
    "anthropology",     "art criticism",         "literary theory",
    "military strategy","ecology",               "neuroscience",
    "political science","theology",
]

STANCES = [
    "skeptic",
    "optimist",
    "pragmatist",
    "systems thinker",
    "first principles reasoner",
]

# Viscacha setup 

client = Client(url=SERVER)
worker = Worker(client)
ai     = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# job handler 

@worker.job("consult_expert", max_retries=2, lease_ttl=60.0)
def consult_expert(question: str, discipline: str, stance: str) -> dict:
    prompt = (
        f"You are a world-class {discipline} expert. "
        f"Your natural way of thinking is that of a {stance}. "
        f"Answer the following question in exactly 2-3 sentences. "
        f"Draw on your specific field — do not speak in generalities. "
        f"Do not introduce yourself or restate the question.\n\n"
        f"Question: {question}"
    )
    resp = ai.messages.create(
        model=MODEL,
        max_tokens=160,
        messages=[{"role": "user", "content": prompt}],
    )
    return {
        "discipline": discipline,
        "stance":     stance,
        "insight":    resp.content[0].text.strip(),
    }

#  synth func

def synthesize(question: str, perspectives: list[dict]) -> str:
    block = "\n\n".join(
        f"[{p['discipline'].upper()} | {p['stance']}]\n{p['insight']}"
        for p in sorted(perspectives, key=lambda x: x["discipline"])
    )
    prompt = (
        f"100 expert agents from different disciplines have answered this question:\n\n"
        f'"{question}"\n\n'
        f"Here are all their perspectives:\n\n{block}\n\n"
        f"Synthesize these into a comprehensive answer. Your response should:\n"
        f"1. Open with the core tension or consensus across disciplines\n"
        f"2. Highlight 3-4 genuinely surprising or non-obvious insights\n"
        f"3. Note where disciplines sharply disagree and why\n"
        f"4. Close with the most actionable or important takeaway\n\n"
        f"Write 4-5 paragraphs. Be specific — name the disciplines and their arguments."
    )
    resp = ai.messages.create(
        model=MODEL,
        max_tokens=800,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.content[0].text.strip()

# helpers 

def print_divider(char="=", width=70):
    print(char * width)

def print_wrapped(text: str, indent: int = 2, width: int = 68):
    for para in text.split("\n"):
        if para.strip():
            print(textwrap.fill(para, width=width,
                                initial_indent=" " * indent,
                                subsequent_indent=" " * indent))
        else:
            print()


def main():
    print_divider()
    print("  THE VISCACHA ORACLE")
    print(f"  100 expert agents | {SERVER} | AI")
    print_divider()
    print()

    question = input("  Your question: ").strip()
    if not question:
        question = (
            "Is artificial general intelligence an existential risk, "
            "an existential opportunity, or neither?"
        )
        print(f"\n  Using default: {question}")

    print(f"\n  Dispatching 100 agents to {SERVER}...\n")

    worker.run(blocking=False, concurrency=WORKERS)

    handles = {}
    for discipline in DISCIPLINES:
        for stance in STANCES:
            key = (discipline, stance)
            handles[key] = client.enqueue(
                "consult_expert",
                question=question,
                discipline=discipline,
                stance=stance,
            )

    print(f"  100 jobs queued across {len(DISCIPLINES)} disciplines x {len(STANCES)} stances.")
    print(f"  {WORKERS} concurrent workers processing...\n")

    with GUIDashboard(client) as dash:
        for h in handles.values():
            try:
                h.wait(timeout=180)
            except TimeoutError:
                pass

    # results 

    perspectives = []
    failed       = []
    for (discipline, stance), h in handles.items():
        r = h.status()
        if r and r.status == "done":
            perspectives.append(r.result)
        else:
            failed.append(f"{discipline}/{stance}")

    print(f"\n  {len(perspectives)}/100 agents responded", end="")
    if failed:
        print(f"  ({len(failed)} failed: {', '.join(failed[:3])}{'...' if len(failed) > 3 else ''})")
    else:
        print()

    if not perspectives:
        print("\n  No results. Check ANTHROPIC_API_KEY and that the server is running.")
        return

    # perspectives 

    print()
    print_divider("-")
    print("  SAMPLE PERSPECTIVES")
    print_divider("-")

    shown = {}
    for p in perspectives:
        if p["discipline"] not in shown:
            shown[p["discipline"]] = p
        if len(shown) >= 6:
            break

    for p in shown.values():
        print(f"\n  [{p['discipline'].upper()} | {p['stance']}]")
        print_wrapped(p["insight"])

    remaining = len(perspectives) - len(shown)
    print(f"\n  ... and {remaining} more perspectives.\n")

    #  synthesi

    print_divider("-")
    print("  SYNTHESIZING 100 PERSPECTIVES...")
    print_divider("-")
    print()

    synthesis = synthesize(question, perspectives)

    print_divider()
    print(f"  ORACLE ANSWER")
    print_divider()
    print(f'\n  Q: "{question}"\n')
    print_wrapped(synthesis, indent=2, width=68)
    print()
    print_divider()
    print(f"  {len(perspectives)} agents | {WORKERS} parallel workers | viscacha-rs + Claude")
    print_divider()
    print()

    dash.wait()


if __name__ == "__main__":
    start = time.time()
    main()
    print(time.time() - start)
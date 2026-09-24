"""Generate article prose from Python-owned facts with structured OpenAI output."""
import argparse
import hashlib
import json
import re
import time

from pydantic import BaseModel, ConfigDict, Field

from .common import config, read_jsonl, resolve, write_json, write_jsonl


class ArticleBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    explanation_summary: str = Field(min_length=80)
    symptoms: list[str] = Field(min_length=2)
    possible_causes: list[str] = Field(min_length=2)
    troubleshooting_steps: list[str] = Field(min_length=3)
    workaround: str = Field(min_length=30)
    resolution: str = Field(min_length=30)
    prevention: list[str] = Field(min_length=2)
    agent_hints: list[str] = Field(min_length=2)
    related_questions: list[str] = Field(min_length=2)
    search_terms: list[str] = Field(min_length=3)


IDENTIFIER = re.compile(r"\b(?:PRD|CUST|SUB|PAY|TKT|EVT|INC|KA)-[A-Za-z0-9-]+\b|\b[A-Z][A-Z0-9]+(?:_[A-Z0-9]+)+\b")


def validate_body(body):
    body = ArticleBody.model_validate(body)
    prose = json.dumps(body.model_dump())
    if IDENTIFIER.search(prose):
        raise ValueError("LLM prose contains an identifier or error code; only Python metadata may contain these")
    return body


def generate_one(metadata, model, cache_dir, client, retries=3, sleep=time.sleep):
    cache_key = hashlib.sha256(json.dumps({"v": 1, "model": model, "facts": metadata}, sort_keys=True).encode()).hexdigest()
    cache_file = cache_dir / f"{cache_key}.json"
    if cache_file.exists():
        body = validate_body(json.loads(cache_file.read_text(encoding="utf-8")))
        return {**metadata, **body.model_dump(), "model": model, "cache_key": cache_key}
    # No IDs or error codes go into the prompt. They remain authoritative metadata.
    facts = {key: metadata[key] for key in ("title", "product_name", "service", "region", "facts")}
    instructions = ("Write a detailed customer-support knowledge article grounded only in the supplied facts. "
                    "Do not assert an unverified root cause, fix, or deployment action. Phrase causes as possibilities, "
                    "and make troubleshooting safe and actionable. Do not write product IDs, ticket IDs, incident IDs, "
                    "error codes, or relationships: Python attaches those after validation. "
                    "Do not invent named products or regions. Include useful details in every section.")
    last_error = None
    for attempt in range(retries):
        try:
            response = client.responses.parse(model=model,
                                              input=[{"role": "system", "content": instructions},
                                                     {"role": "user", "content": json.dumps(facts, sort_keys=True)}],
                                              text_format=ArticleBody)
            if response.output_parsed is None:
                raise ValueError("Model returned no parsed article")
            body = validate_body(response.output_parsed)
            write_json(cache_file, body.model_dump())
            return {**metadata, **body.model_dump(), "model": model, "cache_key": cache_key}
        except Exception as exc:
            last_error = exc
            if attempt + 1 < retries:
                sleep(min(2 ** attempt, 4))
    raise RuntimeError(f"Article {metadata['article_id']} failed after {retries} attempts: {last_error}") from last_error


def generate(cfg, model=None, limit=None, output=None, client=None, skip=False, ingestion_type="all"):
    if skip or cfg["knowledge"]["skip"] and client is None and model is None:
        return {"skipped": True, "articles": 0}
    if ingestion_type not in ("initial", "batch", "all"):
        raise ValueError("ingestion_type must be initial, batch, or all")
    if output and ingestion_type == "all":
        raise ValueError("--output requires --ingestion-type initial or batch")
    if client is None:
        from openai import OpenAI
        client = OpenAI()
    root = resolve(cfg["generated_dir"])
    cache_dir = root / "knowledge_cache"
    cache_dir.mkdir(exist_ok=True)
    chosen_model = model or cfg["knowledge"]["model"]
    kinds = ("initial", "batch") if ingestion_type == "all" else (ingestion_type,)
    outputs = {}
    for kind in kinds:
        metadata = read_jsonl(root / f"knowledge_metadata_{kind}.jsonl")
        if limit is not None:
            metadata = metadata[:limit]
        articles = [generate_one(row, chosen_model, cache_dir, client, cfg["knowledge"]["retries"]) for row in metadata]
        path = resolve(output) if output else root / f"knowledge_articles_{kind}.jsonl"
        write_jsonl(path, articles)
        outputs[kind] = {"articles": len(articles), "output": str(path)}
    return {"skipped": False, "model": chosen_model, "outputs": outputs}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config")
    p.add_argument("--ingestion-type", choices=["initial", "batch", "all"], default="all")
    p.add_argument("--model", help="Override the default gpt-5.6-luna model")
    p.add_argument("--limit", type=int)
    p.add_argument("--output")
    p.add_argument("--skip", action="store_true")
    args = p.parse_args()
    cfg = config(args.config)
    # Explicit CLI invocation runs generation unless --skip; config skip controls embedded/local workflows.
    print(generate(cfg, args.model or cfg["knowledge"]["model"], args.limit, args.output,
                   skip=args.skip, ingestion_type=args.ingestion_type))


if __name__ == "__main__":
    main()

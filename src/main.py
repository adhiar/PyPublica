import csv
import importlib
import json
import os
import queue
import re
import sys
import time
import traceback
import threading
import tkinter as tk
import webbrowser
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import quote, urljoin

import requests

try:
	keyring = importlib.import_module("keyring")
except Exception:
	keyring = None

try:
	unpywall_module = importlib.import_module("unpywall")
	Unpywall = getattr(unpywall_module, "Unpywall", None)
	UnpywallCredentials = getattr(unpywall_module, "UnpywallCredentials", None)
except Exception:
	Unpywall = None
	UnpywallCredentials = None


SCOPUS_SEARCH_URL = "https://api.elsevier.com/content/search/scopus"
ELSEVIER_ARTICLE_BY_DOI_URL = "https://api.elsevier.com/content/article/doi/{doi}"

PUBMED_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
PUBMED_EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
EUROPE_PMC_SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
PMC_IDCONV_URL = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
ICITE_API_URL = "https://icite.od.nih.gov/api/pubs"
DEFAULT_OUTPUT_DIR = Path("output/")
DEFAULT_RESULT_CONTENT_DIR = DEFAULT_OUTPUT_DIR / "result_content"
SEARCH_HISTORY_FILE = DEFAULT_OUTPUT_DIR / "search_history.json"
USER_DOCUMENTS_DIR = Path.home() / "Documents"
APP_CONFIG_DIR = USER_DOCUMENTS_DIR / "pyPaper"
LEGACY_CREDENTIAL_FILES = (
	APP_CONFIG_DIR / "credentials.json",
	DEFAULT_OUTPUT_DIR / "credentials.json",
)
KEYRING_SERVICE_NAME = "pyPublica"
KEYRING_FIELDS = (
	"elsevier_api_key",
	"elsevier_bearer_token",
	"ncbi_email",
	"ncbi_api_key",
)
CREDENTIAL_ENV_KEYS = (
	"ELSEVIER_API_KEY",
	"ELSEVIER_BEARER_TOKEN",
	"NCBI_EMAIL",
	"NCBI_API_KEY",
)
OUTPUT_SUBDIR_NAME = "Output"


def find_app_icon() -> Optional[Path]:
	search_roots: List[Path] = []
	meipass = getattr(sys, "_MEIPASS", None)
	if meipass:
		search_roots.append(Path(meipass))
	if getattr(sys, "frozen", False):
		exe_dir = Path(sys.executable).resolve().parent
		search_roots.append(exe_dir)
		search_roots.append(exe_dir / "_internal")
	search_roots.append(Path(__file__).resolve().parent)
	search_roots.append(Path.cwd())

	candidate_paths = (
		Path("logo.ico"),
		Path("Naminano") / "logo.ico",
	)

	for root in search_roots:
		for candidate in candidate_paths:
			icon_path = root / candidate
			if icon_path.exists():
				return icon_path
	return None


def set_window_icon(window: tk.Misc) -> None:
	icon_path = find_app_icon()
	if not icon_path:
		return
	try:
		window.iconbitmap(default=str(icon_path))
	except Exception:
		# Keep the GUI running even when icon format/path is unsupported in this context.
		return


def normalize_doi(doi: str) -> str:
	return doi.strip().lower()


def ensure_dir(path: Path) -> None:
	path.mkdir(parents=True, exist_ok=True)


def normalize_output_container(path: Path) -> Path:
	candidate = Path(path).expanduser()
	if candidate.name.strip().lower() == OUTPUT_SUBDIR_NAME.lower():
		return candidate
	return candidate / OUTPUT_SUBDIR_NAME


def ensure_output_container(path: Path) -> Path:
	target = normalize_output_container(path)
	ensure_dir(target)
	return target


def safe_filename(value: str) -> str:
	cleaned = []
	for char in value:
		if char.isalnum() or char in {"-", "_", "."}:
			cleaned.append(char)
		else:
			cleaned.append("_")
	return "".join(cleaned)


def normalize_file_name_mode(value: str) -> str:
	mode = str(value or "").strip().lower()
	return "title" if mode == "title" else "doi"


def build_pdf_base_name(record: Dict[str, str], file_name_mode: str) -> str:
	mode = normalize_file_name_mode(file_name_mode)
	doi = str(record.get("doi", "") or "").strip()
	title = str(record.get("title", "") or "").strip()

	if mode == "title" and title:
		selected = title
	elif doi:
		selected = doi
	elif title:
		selected = title
	else:
		selected = "paper"

	base = safe_filename(selected).strip("._")
	if base:
		return base

	fallback = safe_filename(doi or title or "paper").strip("._")
	return fallback or "paper"


def build_pdf_name_candidates(record: Dict[str, str], file_name_mode: str) -> List[str]:
	mode = normalize_file_name_mode(file_name_mode)
	base_name = build_pdf_base_name(record, mode)
	doi_safe = safe_filename(str(record.get("doi", "") or "").strip()).strip("._")

	candidates: List[str] = [base_name]
	if mode == "title" and doi_safe and doi_safe != base_name:
		candidates.append(f"{base_name}__{doi_safe}")
	if doi_safe and doi_safe not in candidates:
		candidates.append(doi_safe)

	return [name for name in candidates if name]


def find_existing_pdf_path(output_dir: Path, record: Dict[str, str], file_name_mode: str) -> Optional[Path]:
	mode = normalize_file_name_mode(file_name_mode)
	candidates = build_pdf_name_candidates(record, mode)

	if mode == "title":
		doi_safe = safe_filename(str(record.get("doi", "") or "").strip()).strip("._")
		if doi_safe:
			base_title = build_pdf_base_name(record, "title")
			for name in (f"{base_title}__{doi_safe}", doi_safe):
				if not name:
					continue
				path = output_dir / f"{name}.pdf"
				if path.exists():
					return path
			return None

	for name in candidates:
		path = output_dir / f"{name}.pdf"
		if path.exists():
			return path
	return None


def resolve_pdf_output_path(
	output_dir: Path,
	record: Dict[str, str],
	file_name_mode: str,
	reserved_paths: Optional[Set[Path]] = None,
) -> Path:
	mode = normalize_file_name_mode(file_name_mode)
	candidates = build_pdf_name_candidates(record, mode)
	reserved = reserved_paths if reserved_paths is not None else set()

	for name in candidates:
		path = output_dir / f"{name}.pdf"
		if path in reserved:
			continue
		if mode == "title" and path.exists():
			continue
		return path

	seed = candidates[0] if candidates else "paper"
	counter = 2
	while True:
		path = output_dir / f"{seed}_{counter}.pdf"
		if path in reserved:
			counter += 1
			continue
		return path


def split_keywords(raw_keywords: str) -> List[str]:
	parts = re.split(r"[;,\n]+", raw_keywords)
	return [part.strip() for part in parts if part.strip()]


def normalize_boolean_expression(expression: str) -> str:
	normalized = re.sub(r"\s+", " ", expression).strip()
	normalized = re.sub(r"\b(and|or|not)\b", lambda m: m.group(1).upper(), normalized, flags=re.IGNORECASE)
	return normalized


def build_keyword_query(
	raw_keywords: str,
	logic: str,
	custom_expression: Optional[str] = None,
) -> str:
	selected_logic = logic.strip().upper()
	raw = (raw_keywords or "").strip()

	if selected_logic == "CUSTOM":
		expression = (custom_expression or "").strip()
		if not expression:
			raise ValueError("Custom expression is empty.")
		return normalize_boolean_expression(expression)

	if not raw:
		raise ValueError("At least one keyword is required.")

	if selected_logic == "AUTO":
		if re.search(r"\b(and|or|not)\b", raw, flags=re.IGNORECASE) or "(" in raw or ")" in raw:
			return normalize_boolean_expression(raw)
		if re.search(r"[;,\n]", raw):
			keywords = split_keywords(raw)
			wrapped = [f'"{kw}"' if " " in kw else kw for kw in keywords]
			if len(wrapped) == 1:
				return wrapped[0]
			# In AUTO mode, separated keyword lists are treated as strict conjunction
			# to avoid broad matches that only satisfy one token.
			return f"({' AND '.join(wrapped)})"
		return f'"{raw}"' if " " in raw else raw

	keywords = split_keywords(raw)
	if not keywords:
		raise ValueError("At least one keyword is required.")
	if len(keywords) == 1 and re.search(r"\b(and|or|not)\b", keywords[0], flags=re.IGNORECASE):
		return normalize_boolean_expression(keywords[0])

	operator = " OR " if selected_logic == "OR" else " AND "
	wrapped = [f'"{kw}"' if " " in kw else kw for kw in keywords]
	if len(wrapped) == 1:
		return wrapped[0]
	return f"({operator.join(wrapped)})"


def extract_year(value: str) -> str:
	if not value:
		return ""
	match = re.search(r"(19|20)\d{2}", value)
	return match.group(0) if match else ""


def normalize_publication_type(raw_type: str, fallback_source: str = "") -> str:
	low = (raw_type or "").strip().lower()
	if not low:
		return "Article"
	if "review" in low or low in {"re", "review-article"}:
		return "Review Journal"
	if "book chapter" in low or "chapter" in low or low in {"ch", "bk-chapter"}:
		return "Book Chapter"
	if "article" in low or "journal" in low or low in {"ar", "ja"}:
		return "Article"
	if fallback_source == "pubmed":
		return "Article"
	return "Article"


def calculate_per_year(cites: str, year: str) -> str:
	if not cites or not year:
		return ""
	try:
		cites_num = float(cites)
		year_num = int(year)
		span = max(datetime.now().year - year_num + 1, 1)
		return f"{(cites_num / span):.2f}"
	except (ValueError, TypeError):
		return ""


def parse_float(value: object) -> float:
	if value is None:
		return 0.0
	text = str(value).strip().replace(",", "")
	if not text:
		return 0.0
	try:
		return float(text)
	except ValueError:
		return 0.0


def rank_records_by_cites(
	records: List[Dict[str, str]],
	limit: Optional[int] = None,
) -> List[Dict[str, str]]:
	def _rank_key(record: Dict[str, str]) -> Tuple[int, float, float]:
		cites = parse_float(record.get("cites", ""))
		year = extract_year(record.get("date", ""))
		if year:
			span = max(datetime.now().year - int(year) + 1, 1)
			per_year = cites / span
		else:
			per_year = 0.0
		# Rank strictly by citation signals, not by recency.
		return (1 if cites > 0 else 0, cites, per_year)

	ranked = sorted(records, key=_rank_key, reverse=True)
	if limit is not None and limit > 0:
		ranked = ranked[:limit]

	with_rank: List[Dict[str, str]] = []
	for index, record in enumerate(ranked, start=1):
		new_record = dict(record)
		new_record["rank"] = str(index)
		with_rank.append(new_record)

	return with_rank


def extract_date_parts(value: str) -> Tuple[int, int, int]:
	if not value:
		return (0, 0, 0)
	iso = re.search(r"((19|20)\d{2})[-/](\d{1,2})[-/](\d{1,2})", value)
	if iso:
		return (int(iso.group(1)), int(iso.group(3)), int(iso.group(4)))
	year = extract_year(value)
	if year:
		return (int(year), 0, 0)
	return (0, 0, 0)


def sort_records_by_mode(records: List[Dict[str, str]], ranking_mode: str) -> List[Dict[str, str]]:
	mode = (ranking_mode or "most_cited").strip().lower()

	def _cites_metrics(record: Dict[str, str]) -> Tuple[float, float]:
		cites = parse_float(record.get("cites", ""))
		year = extract_year(record.get("date", ""))
		if year:
			span = max(datetime.now().year - int(year) + 1, 1)
			per_year = cites / span
		else:
			per_year = 0.0
		return cites, per_year

	if mode == "newest":
		return sorted(
			records,
			key=lambda record: (
				extract_date_parts(record.get("date", "")),
				_cites_metrics(record)[0],
			),
			reverse=True,
		)

	return sorted(
		records,
		key=lambda record: (
			1 if _cites_metrics(record)[0] > 0 else 0,
			_cites_metrics(record)[0],
			-extract_date_parts(record.get("date", ""))[0],
			-extract_date_parts(record.get("date", ""))[1],
			-extract_date_parts(record.get("date", ""))[2],
		),
		reverse=True,
	)


def rank_records(
	records: List[Dict[str, str]],
	ranking_mode: str,
	limit: Optional[int] = None,
) -> List[Dict[str, str]]:
	ranked = sort_records_by_mode(records, ranking_mode)

	if limit is not None and limit > 0:
		ranked = ranked[:limit]

	with_rank: List[Dict[str, str]] = []
	for index, record in enumerate(ranked, start=1):
		new_record = dict(record)
		new_record["rank"] = str(index)
		with_rank.append(new_record)

	return with_rank


def parse_query_terms(raw_keywords: str) -> List[str]:
	raw = (raw_keywords or "").strip()
	if not raw:
		return []
	parts = re.split(r"[;,\n]+|\bAND\b|\bOR\b", raw, flags=re.IGNORECASE)
	terms: List[str] = []
	for part in parts:
		term = part.strip().strip('"').strip("'").strip("()")
		if term:
			terms.append(term.lower())
	return list(dict.fromkeys(terms))


def is_year_range_subset(
	prev_start: Optional[int],
	prev_end: Optional[int],
	new_start: Optional[int],
	new_end: Optional[int],
) -> bool:
	prev_low = prev_start if prev_start is not None else -10_000
	prev_high = prev_end if prev_end is not None else 10_000
	new_low = new_start if new_start is not None else -10_000
	new_high = new_end if new_end is not None else 10_000
	return new_low >= prev_low and new_high <= prev_high


def filter_records_locally(
	records: List[Dict[str, str]],
	terms: List[str],
	title_filter: str,
	start_year: Optional[int],
	end_year: Optional[int],
) -> List[Dict[str, str]]:
	filtered: List[Dict[str, str]] = []
	title_text = (title_filter or "").strip().lower()

	for record in records:
		haystack = " ".join(
			[
				str(record.get("title", "") or ""),
				str(record.get("journal", "") or ""),
				str(record.get("authors", "") or ""),
				str(record.get("publisher", "") or ""),
			]
		).lower()

		if terms and not all(term in haystack for term in terms):
			continue

		title_value = str(record.get("title", "") or "").lower()
		if title_text and title_text not in title_value:
			continue

		year_str = extract_year(str(record.get("date", "") or ""))
		year_val = int(year_str) if year_str else None
		if start_year is not None and (year_val is None or year_val < start_year):
			continue
		if end_year is not None and (year_val is None or year_val > end_year):
			continue

		filtered.append(record)

	return filtered


def request_with_retry(
	session: requests.Session,
	method: str,
	url: str,
	*,
	retries: int = 3,
	sleep_seconds: float = 1.0,
	**kwargs,
) -> requests.Response:
	last_error = None
	for attempt in range(1, retries + 1):
		try:
			response = session.request(method, url, timeout=60, **kwargs)
			if response.status_code in {429, 500, 502, 503, 504}:
				raise requests.HTTPError(
					f"Transient HTTP error {response.status_code}", response=response
				)
			response.raise_for_status()
			return response
		except (requests.RequestException, requests.HTTPError) as error:
			last_error = error
			if attempt == retries:
				break
			time.sleep(sleep_seconds * attempt)
	raise RuntimeError(f"Request failed after {retries} attempts: {url}\n{last_error}")


def build_scopus_query(
	base_query: str,
	start_year: Optional[int],
	end_year: Optional[int],
	oapen_access_only: bool = True,
) -> str:
	query = base_query.strip()
	if oapen_access_only:
		query += " AND OPENACCESS(1)"
	if start_year and end_year:
		query += f" AND PUBYEAR > {start_year - 1} AND PUBYEAR < {end_year + 1}"
	elif start_year:
		query += f" AND PUBYEAR > {start_year - 1}"
	elif end_year:
		query += f" AND PUBYEAR < {end_year + 1}"
	return query


def search_elsevier_scopus_dois(
	session: requests.Session,
	api_key: str,
	query: str,
	start_year: Optional[int],
	end_year: Optional[int],
	max_results: int,
	ranking_mode: str = "most_cited",
	bearer_token: Optional[str] = None,
	count_per_page: int = 25,
	progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> List[Dict[str, str]]:
	all_records: List[Dict[str, str]] = []
	start = 0
	mode = (ranking_mode or "most_cited").strip().lower()
	fetch_target = max_results
	if mode == "most_cited":
		# Avoid newest-page bias by collecting a wider candidate pool, then ranking.
		fetch_target = min(max(max_results * 8, 300), 800)
	final_query = build_scopus_query(query, start_year, end_year, oapen_access_only=True)

	headers = {
		"X-ELS-APIKey": api_key,
		"Accept": "application/json",
	}
	if bearer_token:
		headers["Authorization"] = f"Bearer {bearer_token}"

	while len(all_records) < fetch_target:
		params = {
			"query": final_query,
			"start": start,
			"count": min(count_per_page, fetch_target - len(all_records)),
			"apiKey": api_key,
			"field": "dc:identifier,dc:title,dc:creator,dc:publisher,prism:doi,prism:coverDate,prism:publicationName,openaccess,freetoread,citedby-count,subtype,subtypeDescription",
		}
		if mode == "most_cited":
			params["sort"] = "citedby-count"
		response = request_with_retry(
			session,
			"GET",
			SCOPUS_SEARCH_URL,
			headers=headers,
			params=params,
		)
		payload = response.json()
		entries = payload.get("search-results", {}).get("entry", [])
		if not entries:
			break

		for item in entries:
			doi = item.get("prism:doi")
			if not doi:
				continue

			open_access_value = str(item.get("openaccess", "")).strip().lower()
			if open_access_value not in {"1", "true", "yes"}:
				continue

			all_records.append(
				{
					"doi": normalize_doi(doi),
					"source": "elsevier",
					"open_access": open_access_value,
					"title": item.get("dc:title", ""),
					"date": item.get("prism:coverDate", ""),
					"journal": item.get("prism:publicationName", ""),
					"authors": item.get("dc:creator", ""),
					"publisher": item.get("dc:publisher", ""),
					"cites": str(item.get("citedby-count", "") or ""),
					"type": item.get("subtypeDescription", "") or item.get("subtype", ""),
					"id": item.get("dc:identifier", ""),
				}
			)

		start += len(entries)
		if progress_callback is not None:
			progress_callback(
				min(len(all_records), fetch_target),
				fetch_target,
				f"Elsevier query progress: {min(len(all_records), fetch_target)}/{fetch_target}",
			)
		if len(entries) < params["count"]:
			break

	if len(all_records) > max_results:
		all_records = sort_records_by_mode(all_records, ranking_mode)[:max_results]

	return all_records


def build_pubmed_term(
	base_query: str,
	start_year: Optional[int],
	end_year: Optional[int],
	medline_only: bool = True,
	open_access_only: bool = True,
) -> str:
	query_parts = [f"({base_query.strip()})", "(hasabstract[text])"]
	if medline_only:
		query_parts.append("(medline[sb])")
	if open_access_only:
		# Prefer records that are more likely to have downloadable full text.
		query_parts.append('("open access"[filter] OR "free full text"[sb])')
	if start_year and end_year:
		query_parts.append(f'("{start_year}"[Date - Publication] : "{end_year}"[Date - Publication])')
	elif start_year:
		query_parts.append(f'("{start_year}"[Date - Publication] : "3000"[Date - Publication])')
	elif end_year:
		query_parts.append(f'("1800"[Date - Publication] : "{end_year}"[Date - Publication])')
	return " AND ".join(query_parts)


def esearch_pubmed_ids(
	session: requests.Session,
	term: str,
	max_results: int,
	email: Optional[str],
	api_key: Optional[str],
) -> List[str]:
	retstart = 0
	retmax = 200
	ids: List[str] = []

	while len(ids) < max_results:
		params = {
			"db": "pubmed",
			"term": term,
			"retmode": "json",
			"retmax": min(retmax, max_results - len(ids)),
			"retstart": retstart,
		}
		if email:
			params["email"] = email
		if api_key:
			params["api_key"] = api_key

		response = request_with_retry(session, "GET", PUBMED_ESEARCH_URL, params=params)
		data = response.json().get("esearchresult", {})
		page_ids = data.get("idlist", [])
		if not page_ids:
			break
		ids.extend(page_ids)
		retstart += len(page_ids)
		if len(page_ids) < params["retmax"]:
			break

	return ids


def fetch_pubmed_records_xml(
	session: requests.Session,
	pmids: List[str],
	email: Optional[str],
	api_key: Optional[str],
) -> str:
	params = {
		"db": "pubmed",
		"id": ",".join(pmids),
		"retmode": "xml",
	}
	if email:
		params["email"] = email
	if api_key:
		params["api_key"] = api_key

	response = request_with_retry(session, "GET", PUBMED_EFETCH_URL, params=params)
	return response.text


def parse_pubmed_doi_map(pubmed_xml: str) -> List[Dict[str, str]]:
	records: List[Dict[str, str]] = []
	seen_titles: Set[str] = set()
	root = ET.fromstring(pubmed_xml)

	for article in root.findall(".//PubmedArticle"):
		pmid_node = article.find(".//MedlineCitation/PMID")
		pmid = pmid_node.text.strip() if pmid_node is not None and pmid_node.text else ""

		title_node = article.find(".//Article/ArticleTitle")
		title = "".join(title_node.itertext()).strip() if title_node is not None else ""

		# Skip duplicate titles
		title_key = title.lower()
		if title_key in seen_titles:
			continue

		journal_node = article.find(".//Article/Journal/Title")
		journal = journal_node.text.strip() if journal_node is not None and journal_node.text else ""

		year_node = article.find(".//Article/Journal/JournalIssue/PubDate/Year")
		year = year_node.text.strip() if year_node is not None and year_node.text else ""
		if not year:
			medline_date = article.find(".//Article/Journal/JournalIssue/PubDate/MedlineDate")
			year = extract_year(medline_date.text if medline_date is not None and medline_date.text else "")

		author_values: List[str] = []
		for author in article.findall(".//Article/AuthorList/Author"):
			collective = author.find("CollectiveName")
			if collective is not None and collective.text:
				author_values.append(collective.text.strip())
				continue
			last_name = author.find("LastName")
			fore_name = author.find("ForeName")
			if last_name is not None and last_name.text:
				if fore_name is not None and fore_name.text:
					author_values.append(f"{last_name.text.strip()} {fore_name.text.strip()}")
				else:
					author_values.append(last_name.text.strip())
		authors_text = ", ".join(author_values)

		pub_types: List[str] = []
		for ptype in article.findall(".//Article/PublicationTypeList/PublicationType"):
			if ptype.text:
				pub_types.append(ptype.text.strip())
		type_text = normalize_publication_type("; ".join(pub_types), fallback_source="pubmed")

		# Only take the first DOI (the article's own DOI), skip reference DOIs
		doi_node = article.find('.//ArticleIdList/ArticleId[@IdType="doi"]')
		if doi_node is not None and doi_node.text:
			seen_titles.add(title_key)
			records.append(
				{
					"doi": normalize_doi(doi_node.text),
					"source": "pubmed",
					# PubMed query is already constrained with OA/free-full-text filters.
					"open_access": "1",
					"title": title,
					"date": year,
					"journal": journal,
					"authors": authors_text,
					"publisher": "",
					"cites": "",
					"type": type_text,
					"id": pmid,
				}
			)

	return records


def search_pubmed_dois(
	session: requests.Session,
	query: str,
	start_year: Optional[int],
	end_year: Optional[int],
	max_results: int,
	email: Optional[str],
	api_key: Optional[str],
	progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> List[Dict[str, str]]:
	term = build_pubmed_term(query, start_year, end_year)
	pmids = esearch_pubmed_ids(session, term, max_results=max_results, email=email, api_key=api_key)
	if not pmids:
		return []
	if progress_callback is not None:
		progress_callback(0, len(pmids), f"PubMed IDs found: {len(pmids)}")

	all_records: List[Dict[str, str]] = []
	chunk_size = 100
	for i in range(0, len(pmids), chunk_size):
		chunk = pmids[i : i + chunk_size]
		xml_text = fetch_pubmed_records_xml(session, chunk, email=email, api_key=api_key)
		parsed = parse_pubmed_doi_map(xml_text)
		all_records.extend(parsed)
		if progress_callback is not None:
			progress_callback(
				min(len(all_records), len(pmids)),
				len(pmids),
				f"PubMed fetch progress: {min(len(all_records), len(pmids))}/{len(pmids)}",
			)
		time.sleep(0.34 if api_key else 0.5)

	return all_records[:max_results]


def fetch_pubmed_cites_from_europe_pmc(
	session: requests.Session,
	pmid: str,
	doi: str,
	logger: Optional[Callable[[str], None]] = None,
) -> str:
	log = _resolve_logger(logger)
	query = ""
	if pmid:
		query = f"EXT_ID:{pmid} AND SRC:MED"
	elif doi:
		query = f'DOI:"{doi}"'
	else:
		return ""

	try:
		response = request_with_retry(
			session,
			"GET",
			EUROPE_PMC_SEARCH_URL,
			headers={"Accept": "application/json"},
			params={
				"query": query,
				"format": "json",
				"pageSize": 1,
			},
		)
		results = response.json().get("resultList", {}).get("result", [])
		if not results:
			return ""
		cited_by = results[0].get("citedByCount")
		if cited_by is None:
			return ""
		return str(cited_by)
	except RuntimeError as error:
		log(f"[WARN] Europe PMC citation lookup failed for PMID={pmid} DOI={doi}: {error}")
		return ""


def fetch_doi_cites_from_crossref(
	session: requests.Session,
	doi: str,
	logger: Optional[Callable[[str], None]] = None,
) -> str:
	log = _resolve_logger(logger)
	if not doi:
		return ""
	api_url = f"https://api.crossref.org/works/{quote(doi, safe='/')}"
	try:
		response = request_with_retry(
			session,
			"GET",
			api_url,
			headers={"Accept": "application/json"},
		)
		message = response.json().get("message", {})
		count = message.get("is-referenced-by-count")
		if count is None:
			return ""
		return str(count)
	except RuntimeError as error:
		log(f"[WARN] Crossref citation lookup failed for DOI={doi}: {error}")
		return ""


def fetch_pubmed_cites_from_icite(
	session: requests.Session,
	pmids: List[str],
	logger: Optional[Callable[[str], None]] = None,
) -> Dict[str, str]:
	log = _resolve_logger(logger)
	result: Dict[str, str] = {}
	if not pmids:
		return result

	clean_pmids = [pmid.strip() for pmid in pmids if pmid and pmid.strip()]
	if not clean_pmids:
		return result

	chunk_size = 200
	for i in range(0, len(clean_pmids), chunk_size):
		chunk = clean_pmids[i : i + chunk_size]
		try:
			response = request_with_retry(
				session,
				"GET",
				ICITE_API_URL,
				headers={"Accept": "application/json"},
				params={"pmids": ",".join(chunk)},
			)
			data = response.json().get("data", [])
			for item in data:
				pmid_value = str(item.get("pmid", "") or "").strip()
				cited_value = item.get("citation_count")
				if not pmid_value or cited_value is None:
					continue
				result[pmid_value] = str(cited_value)
		except RuntimeError as error:
			log(f"[WARN] iCite citation lookup failed for PMID chunk starting at {i}: {error}")
			time.sleep(0.2)

	return result


def enrich_pubmed_citations(
	session: requests.Session,
	pubmed_records: List[Dict[str, str]],
	logger: Optional[Callable[[str], None]] = None,
	progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> List[Dict[str, str]]:
	pmids_for_icite = list(
		{
			str(record.get("id", "") or "").strip()
			for record in pubmed_records
			if record.get("source") == "pubmed"
			and not str(record.get("cites", "")).strip()
			and str(record.get("id", "") or "").strip()
		}
	)
	icite_cites = fetch_pubmed_cites_from_icite(session, pmids_for_icite, logger=logger)

	total = len(pubmed_records)
	for index, record in enumerate(pubmed_records, start=1):
		if record.get("source") != "pubmed":
			if progress_callback is not None:
				progress_callback(index, total, f"PubMed citation lookup: {index}/{total}")
			continue
		if str(record.get("cites", "")).strip():
			if progress_callback is not None:
				progress_callback(index, total, f"PubMed citation lookup: {index}/{total}")
			continue
		pmid = str(record.get("id", "") or "").strip()
		doi = str(record.get("doi", "") or "").strip()
		cites = icite_cites.get(pmid, "") if pmid else ""
		if not cites:
			cites = fetch_pubmed_cites_from_europe_pmc(session, pmid=pmid, doi=doi, logger=logger)
		if not cites and doi:
			cites = fetch_doi_cites_from_crossref(session, doi=doi, logger=logger)
		if cites:
			record["cites"] = cites
		else:
			record["cites"] = "0"
		if progress_callback is not None:
			progress_callback(index, total, f"PubMed citation lookup: {index}/{total}")
		time.sleep(0.05)
	return pubmed_records


def deduplicate_records(records: List[Dict[str, str]]) -> List[Dict[str, str]]:
	by_doi: Dict[str, Dict[str, str]] = {}
	prefer_longer_fields = {"title", "authors", "journal", "publisher"}
	for rec in records:
		doi = rec.get("doi", "").strip()
		if not doi:
			continue
		if doi not in by_doi:
			by_doi[doi] = dict(rec)
			continue

		existing = by_doi[doi]
		for key, value in rec.items():
			if value and not existing.get(key):
				existing[key] = value
			elif key in prefer_longer_fields and value:
				existing_text = str(existing.get(key, "") or "").strip()
				new_text = str(value or "").strip()
				if len(new_text) > len(existing_text):
					existing[key] = value
		if rec.get("source") == "elsevier":
			existing["source"] = "elsevier"
	return list(by_doi.values())


def _is_open_access_record(record: Dict[str, str]) -> bool:
	value = str(record.get("open_access", "") or "").strip().lower()
	return value in {"1", "true", "yes", "open", "oa"}


def filter_open_access_records(
	records: List[Dict[str, str]],
	logger: Optional[Callable[[str], None]] = None,
) -> List[Dict[str, str]]:
	log = _resolve_logger(logger) if logger is not None else None
	filtered = [record for record in records if _is_open_access_record(record)]
	dropped = len(records) - len(filtered)
	if log is not None and dropped > 0:
		log(f"[INFO] OA-only filter removed {dropped} non-open-access records.")
	return filtered


def save_doi_csv(records: List[Dict[str, str]], output_csv: Path) -> None:
	ensure_dir(output_csv.parent)
	with output_csv.open("w", newline="", encoding="utf-8") as file:
		writer = csv.DictWriter(
			file,
			fieldnames=[
				"rank",
				"doi",
				"source",
				"open_access",
				"title",
				"date",
				"journal",
				"authors",
				"publisher",
				"cites",
				"type",
				"id",
			],
			extrasaction="ignore",
		)
		writer.writeheader()
		writer.writerows(records)


def get_pubmed_pmid_by_doi(
	session: requests.Session,
	doi: str,
	email: Optional[str],
	api_key: Optional[str],
) -> Optional[str]:
	term = f'"{doi}"[AID]'
	params = {
		"db": "pubmed",
		"term": term,
		"retmode": "json",
		"retmax": 1,
	}
	if email:
		params["email"] = email
	if api_key:
		params["api_key"] = api_key

	response = request_with_retry(session, "GET", PUBMED_ESEARCH_URL, params=params)
	ids = response.json().get("esearchresult", {}).get("idlist", [])
	return ids[0] if ids else None


def _is_elsevier_publisher_doi(doi: str) -> bool:
	"""Return True only if the DOI belongs to an Elsevier journal (10.1016/)."""
	return doi.strip().lower().startswith("10.1016/")


def _is_elsevier_error_response(content: str) -> bool:
	if not content or len(content.strip()) < 50:
		return True
	lower = content[:2000].lower()
	if "<service-error>" in lower:
		return True
	if "<!doctype html" in lower or "<html" in lower:
		return True
	if '"error"' in lower and '"service-error"' in lower:
		return True
	return False


def _resolve_logger(logger: Optional[Callable[[str], None]]) -> Callable[[str], None]:
	return logger if logger is not None else print


def download_elsevier_xml(
	session: requests.Session,
	doi: str,
	api_key: str,
	bearer_token: Optional[str] = None,
	logger: Optional[Callable[[str], None]] = None,
) -> Optional[str]:
	log = _resolve_logger(logger)
	encoded_doi = quote(doi, safe="/")
	url = ELSEVIER_ARTICLE_BY_DOI_URL.format(doi=encoded_doi)
	headers = {
		"X-ELS-APIKey": api_key,
		"Accept": "text/xml",
	}
	if bearer_token:
		headers["Authorization"] = f"Bearer {bearer_token}"

	try:
		response = request_with_retry(session, "GET", url, headers=headers)
		if _is_elsevier_error_response(response.text):
			log(f"[WARN] Elsevier returned error/non-article response for DOI {doi}")
			return None
		return response.text
	except RuntimeError as error:
		log(f"[WARN] Elsevier XML failed for DOI {doi}: {error}")
		return None


def download_elsevier_pdf(
	session: requests.Session,
	doi: str,
	api_key: str,
	bearer_token: Optional[str] = None,
	logger: Optional[Callable[[str], None]] = None,
) -> Optional[bytes]:
	log = _resolve_logger(logger)
	encoded_doi = quote(doi, safe="/")
	url = ELSEVIER_ARTICLE_BY_DOI_URL.format(doi=encoded_doi)
	headers = {
		"X-ELS-APIKey": api_key,
		"Accept": "application/pdf",
	}
	if bearer_token:
		headers["Authorization"] = f"Bearer {bearer_token}"

	try:
		response = request_with_retry(session, "GET", url, headers=headers)
		ctype = response.headers.get("Content-Type", "")
		if "pdf" in ctype or (len(response.content) > 1000 and response.content[:5] == b"%PDF-"):
			return response.content
		log(f"[WARN] Elsevier did not return PDF for DOI {doi} (Content-Type: {ctype})")
		return None
	except RuntimeError as error:
		log(f"[WARN] Elsevier PDF failed for DOI {doi}: {error}")
		return None


def download_via_unpaywall(
	session: requests.Session,
	doi: str,
	email: str,
	logger: Optional[Callable[[str], None]] = None,
) -> Optional[bytes]:
	log = _resolve_logger(logger)
	if not email:
		return None

	candidate_urls: List[str] = []
	seen_urls: Set[str] = set()

	def _add_candidate(url: str) -> None:
		clean = str(url or "").strip()
		if not clean or clean in seen_urls:
			return
		seen_urls.add(clean)
		candidate_urls.append(clean)

	# First try link discovery via the unpywall client if installed.
	if Unpywall is not None:
		try:
			if UnpywallCredentials is not None:
				UnpywallCredentials(email)
			else:
				os.environ["UNPAYWALL_EMAIL"] = email

			all_links = []
			if hasattr(Unpywall, "get_all_links"):
				all_links = Unpywall.get_all_links(doi) or []
			for url in all_links:
				_add_candidate(str(url or ""))

			# Extra explicit link calls in case get_all_links returns partial data.
			if hasattr(Unpywall, "get_pdf_link"):
				_add_candidate(str(Unpywall.get_pdf_link(doi) or ""))
			if hasattr(Unpywall, "get_doc_link"):
				_add_candidate(str(Unpywall.get_doc_link(doi) or ""))
		except Exception as error:
			log(f"[WARN] unpywall client lookup failed for DOI {doi}: {error}")

	api_url = f"https://api.unpaywall.org/v2/{quote(doi, safe='/')}"
	try:
		response = request_with_retry(
			session, "GET", api_url, params={"email": email}
		)
		data = response.json()
		boa = data.get("best_oa_location") or {}
		_add_candidate(str(boa.get("url_for_pdf") or ""))
		_add_candidate(str(boa.get("url") or ""))
		for loc in data.get("oa_locations", []):
			if not isinstance(loc, dict):
				continue
			_add_candidate(str(loc.get("url_for_pdf") or ""))
			_add_candidate(str(loc.get("url") or ""))

		for candidate_url in candidate_urls:
			pdf_data = _download_pdf_from_candidate_url(session, candidate_url, logger=log)
			if pdf_data:
				return pdf_data
		return None
	except (RuntimeError, Exception) as error:
		log(f"[WARN] Unpaywall fallback failed for DOI {doi}: {error}")
		return None


def download_via_crossref_open_access(
	session: requests.Session,
	doi: str,
	logger: Optional[Callable[[str], None]] = None,
) -> Optional[bytes]:
	"""Try to get an open-access PDF link from CrossRef metadata."""
	log = _resolve_logger(logger)
	api_url = f"https://api.crossref.org/works/{quote(doi, safe='/')}"
	try:
		response = request_with_retry(
			session, "GET", api_url,
			headers={"Accept": "application/json"},
		)
		data = response.json().get("message", {})
		# Check for open-access links in the CrossRef record.
		for link in data.get("link", []):
			url = link.get("URL", "")
			if url:
				pdf_data = _download_pdf_from_candidate_url(session, url, logger=log)
				if pdf_data:
					return pdf_data

		# Also try the primary landing page URL in CrossRef metadata.
		resource = data.get("resource") or {}
		primary = resource.get("primary") if isinstance(resource, dict) else None
		primary_url = ""
		if isinstance(primary, dict):
			primary_url = str(primary.get("URL", "") or "").strip()
		if primary_url:
			pdf_data = _download_pdf_from_candidate_url(session, primary_url, logger=log)
			if pdf_data:
				return pdf_data
		return None
	except (RuntimeError, Exception) as error:
		log(f"[WARN] CrossRef fallback failed for DOI {doi}: {error}")
		return None


def _response_has_pdf(response: requests.Response) -> bool:
	content_type = str(response.headers.get("Content-Type", "")).lower()
	return (
		"pdf" in content_type
		or (len(response.content) > 5 and response.content[:5] == b"%PDF-")
	)


def _download_pdf_from_candidate_url(
	session: requests.Session,
	url: str,
	logger: Optional[Callable[[str], None]] = None,
	referer: Optional[str] = None,
) -> Optional[bytes]:
	log = _resolve_logger(logger)
	headers = {
		"User-Agent": "Mozilla/5.0 (DOI-Workflow-Retriever)",
		"Accept": "application/pdf,text/html,application/xhtml+xml,*/*",
	}
	if referer:
		headers["Referer"] = referer

	try:
		response = request_with_retry(
			session,
			"GET",
			url,
			headers=headers,
			allow_redirects=True,
		)
		if _response_has_pdf(response):
			return response.content

		content_type = str(response.headers.get("Content-Type", "")).lower()
		if "html" not in content_type:
			return None

		nested_links = _extract_pdf_links_from_html(response.text or "", str(response.url))
		visited: Set[str] = set()
		for nested_url in nested_links[:20]:
			if nested_url in visited:
				continue
			visited.add(nested_url)
			nested_resp = request_with_retry(
				session,
				"GET",
				nested_url,
				headers={
					"User-Agent": headers["User-Agent"],
					"Accept": "application/pdf,*/*",
					"Referer": str(response.url),
				},
				allow_redirects=True,
			)
			if _response_has_pdf(nested_resp):
				return nested_resp.content

		return None
	except Exception as error:
		log(f"[WARN] Candidate URL PDF fetch failed: {url} ({error})")
		return None


def download_pubmed_open_access_pdf(
	session: requests.Session,
	pmid: str,
	doi: str,
	logger: Optional[Callable[[str], None]] = None,
) -> Optional[bytes]:
	"""Use Europe PMC fullTextUrlList for PubMed records when available."""
	log = _resolve_logger(logger)
	query = ""
	if pmid:
		query = f"EXT_ID:{pmid} AND SRC:MED"
	elif doi:
		query = f'DOI:"{doi}"'
	if not query:
		return None

	try:
		response = request_with_retry(
			session,
			"GET",
			EUROPE_PMC_SEARCH_URL,
			headers={"Accept": "application/json"},
			params={"query": query, "format": "json", "pageSize": 1},
		)
		results = response.json().get("resultList", {}).get("result", [])
		if not results:
			return None

		full_urls = results[0].get("fullTextUrlList", {}).get("fullTextUrl", [])
		for item in full_urls:
			if not isinstance(item, dict):
				continue
			candidate_url = str(item.get("url", "") or "").strip()
			if not candidate_url:
				continue
			pdf_data = _download_pdf_from_candidate_url(session, candidate_url, logger=log)
			if pdf_data:
				return pdf_data
		return None
	except Exception as error:
		log(f"[WARN] PubMed OA fallback failed for PMID={pmid} DOI={doi}: {error}")
		return None


def download_pubmed_pdf_from_pmc(
	session: requests.Session,
	pmid: str,
	doi: str,
	logger: Optional[Callable[[str], None]] = None,
) -> Optional[bytes]:
	"""Resolve PMCID via NCBI idconv and fetch the PMC-hosted PDF."""
	log = _resolve_logger(logger)
	id_candidates: List[str] = []
	if pmid:
		id_candidates.append(pmid)
	if doi:
		id_candidates.append(doi)
	if not id_candidates:
		return None

	try:
		response = request_with_retry(
			session,
			"GET",
			PMC_IDCONV_URL,
			headers={"Accept": "application/json"},
			params={
				"ids": ",".join(id_candidates),
				"format": "json",
			},
		)
		payload = response.json()
		records = payload.get("records", []) if isinstance(payload, dict) else []
		for item in records:
			if not isinstance(item, dict):
				continue
			pmcid = str(item.get("pmcid", "") or "").strip()
			if not pmcid:
				continue
			for candidate_url in (
				f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/pdf/",
				f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/pdf/?download=1",
			):
				pdf_data = _download_pdf_from_candidate_url(session, candidate_url, logger=log)
				if pdf_data:
					return pdf_data
		return None
	except Exception as error:
		log(f"[WARN] PMC PDF fallback failed for PMID={pmid} DOI={doi}: {error}")
		return None


def _extract_pdf_links_from_html(html_text: str, base_url: str) -> List[str]:
	links: List[str] = []
	if not html_text:
		return links

	patterns = [
		r'<meta[^>]+name=["\']citation_pdf_url["\'][^>]+content=["\']([^"\']+)["\']',
		r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']citation_pdf_url["\']',
		r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>',
		r'<link[^>]+href=["\']([^"\']+)["\'][^>]*>',
	]

	seen: Set[str] = set()
	for pattern in patterns:
		for match in re.findall(pattern, html_text, flags=re.IGNORECASE):
			candidate = str(match).strip()
			if not candidate:
				continue
			if candidate.startswith("javascript:"):
				continue
			full_url = urljoin(base_url, candidate)
			low = full_url.lower()
			if (
				".pdf" in low
				or "pdf" in low
				or "download" in low
				or "fulltext" in low
			):
				if full_url not in seen:
					seen.add(full_url)
					links.append(full_url)

	return links


def download_via_doi_landing_page(
	session: requests.Session,
	doi: str,
	logger: Optional[Callable[[str], None]] = None,
) -> Optional[bytes]:
	"""Resolve DOI to publisher page, then discover and fetch PDF links."""
	log = _resolve_logger(logger)
	resolver_url = f"https://doi.org/{quote(doi, safe='/')}"
	headers = {
		"User-Agent": "Mozilla/5.0 (DOI-Workflow-Retriever)",
		"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
	}

	try:
		landing = request_with_retry(
			session,
			"GET",
			resolver_url,
			headers=headers,
			allow_redirects=True,
		)
		if _response_has_pdf(landing):
			return landing.content

		landing_url = str(landing.url)
		landing_html = landing.text or ""
		candidate_links = _extract_pdf_links_from_html(landing_html, landing_url)
		visited: Set[str] = set()

		for pdf_url in candidate_links[:20]:
			if pdf_url in visited:
				continue
			visited.add(pdf_url)

			resp = request_with_retry(
				session,
				"GET",
				pdf_url,
				headers={
					"User-Agent": headers["User-Agent"],
					"Referer": landing_url,
					"Accept": "application/pdf,text/html,*/*",
				},
				allow_redirects=True,
			)
			if _response_has_pdf(resp):
				return resp.content

			content_type = str(resp.headers.get("Content-Type", "")).lower()
			if "html" in content_type:
				nested_links = _extract_pdf_links_from_html(resp.text or "", str(resp.url))
				for nested_url in nested_links[:10]:
					if nested_url in visited:
						continue
					visited.add(nested_url)
					nested_resp = request_with_retry(
						session,
						"GET",
						nested_url,
						headers={
							"User-Agent": headers["User-Agent"],
							"Referer": str(resp.url),
							"Accept": "application/pdf,*/*",
						},
						allow_redirects=True,
					)
					if _response_has_pdf(nested_resp):
						return nested_resp.content

		return None
	except (RuntimeError, Exception) as error:
		log(f"[WARN] DOI landing-page fallback failed for DOI {doi}: {error}")
		return None


def download_pubmed_xml_by_pmid(
	session: requests.Session,
	pmid: str,
	email: Optional[str],
	api_key: Optional[str],
) -> Optional[str]:
	params = {
		"db": "pubmed",
		"id": pmid,
		"retmode": "xml",
	}
	if email:
		params["email"] = email
	if api_key:
		params["api_key"] = api_key
	try:
		response = request_with_retry(session, "GET", PUBMED_EFETCH_URL, params=params)
		return response.text
	except RuntimeError:
		return None


def download_xml_for_records(
	session: requests.Session,
	records: List[Dict[str, str]],
	output_dir: Path,
	elsevier_api_key: Optional[str],
	elsevier_bearer_token: Optional[str],
	ncbi_email: Optional[str],
	ncbi_api_key: Optional[str],
	file_name_mode: str = "doi",
	logger: Optional[Callable[[str], None]] = None,
	progress_callback: Optional[Callable[[int, int, str], None]] = None,
	cancel_requested: Optional[Callable[[], bool]] = None,
) -> Dict[str, object]:
	log = _resolve_logger(logger)
	ensure_dir(output_dir)
	mode = normalize_file_name_mode(file_name_mode)
	reserved_paths: Set[Path] = set()
	stats: Dict[str, object] = {
		"saved": 0,
		"skipped": 0,
		"failed": 0,
		"cancelled": False,
		"failed_items": [],
		"saved_items": [],
	}

	unpaywall_email = ncbi_email or os.getenv("UNPAYWALL_EMAIL", "")

	total_records = len(records)
	for index, record in enumerate(records, start=1):
		if cancel_requested is not None and cancel_requested():
			stats["cancelled"] = True
			if progress_callback is not None:
				progress_callback(index - 1, total_records, f"Cancellation requested. Stopped at {index - 1}/{total_records}.")
			break

		doi = record["doi"]
		source = record["source"]

		def _report_step(step: str, current_value: Optional[int] = None) -> None:
			if progress_callback is None:
				return
			current = index - 1 if current_value is None else current_value
			progress_callback(current, total_records, f"[{index}/{total_records}] {doi} | {step}")

		_report_step(f"Preparing download from source={source}")

		existing_path = find_existing_pdf_path(output_dir, record, mode)
		if existing_path is not None:
			stats["skipped"] = int(stats["skipped"]) + 1
			reserved_paths.add(existing_path)
			_report_step(f"Skipped (already exists: {existing_path.name})", current_value=index)
			continue

		pdf_path = resolve_pdf_output_path(output_dir, record, mode, reserved_paths=reserved_paths)
		reserved_paths.add(pdf_path)
		_report_step(f"Target file: {pdf_path.name} (name mode: {mode.upper()})")

		saved = False
		saved_via = ""
		attempts: List[str] = []

		if source == "elsevier":
			# Strategy 1: Elsevier API only for true Elsevier publisher DOIs.
			if elsevier_api_key and _is_elsevier_publisher_doi(doi):
				attempts.append("Elsevier API")
				_report_step("Trying Elsevier API")
				pdf_data = download_elsevier_pdf(
					session, doi, elsevier_api_key,
					bearer_token=elsevier_bearer_token,
					logger=log,
				)
				if pdf_data:
					pdf_path.write_bytes(pdf_data)
					saved = True
					saved_via = "Elsevier API"
			elif not elsevier_api_key:
				_report_step("Skipping Elsevier API (missing API key)")
				log(f"[INFO] Elsevier API key missing for DOI {doi}, trying open-access fallbacks...")
			elif not _is_elsevier_publisher_doi(doi):
				_report_step("Skipping Elsevier API (DOI is not Elsevier publisher)")
				log(f"[INFO] DOI {doi} is not an Elsevier journal (found via Scopus), trying alternatives...")

			# Strategy 2: Unpaywall fallback
			if not saved and unpaywall_email:
				attempts.append("Unpaywall")
				_report_step("Trying Unpaywall")
				pdf_data = download_via_unpaywall(session, doi, unpaywall_email, logger=log)
				if pdf_data:
					pdf_path.write_bytes(pdf_data)
					saved = True
					saved_via = "Unpaywall"
			elif not saved:
				_report_step("Skipping Unpaywall (missing email)")

			# Strategy 3: CrossRef open-access links
			if not saved:
				attempts.append("CrossRef")
				_report_step("Trying CrossRef")
				pdf_data = download_via_crossref_open_access(session, doi, logger=log)
				if pdf_data:
					pdf_path.write_bytes(pdf_data)
					saved = True
					saved_via = "CrossRef"

			# Strategy 4: Resolve DOI landing page and scrape publisher PDF links
			if not saved:
				attempts.append("DOI landing page")
				_report_step("Trying DOI landing page")
				pdf_data = download_via_doi_landing_page(session, doi, logger=log)
				if pdf_data:
					pdf_path.write_bytes(pdf_data)
					saved = True
					saved_via = "DOI landing page"

		elif source == "pubmed":
			# PubMed download policy: only use Unpaywall (via unpywall client + API fallback).
			if unpaywall_email:
				attempts.append("Unpaywall")
				_report_step("Trying Unpaywall (PubMed policy)")
				pdf_data = download_via_unpaywall(session, doi, unpaywall_email, logger=log)
				if pdf_data:
					pdf_path.write_bytes(pdf_data)
					saved = True
					saved_via = "Unpaywall"
			else:
				attempts.append("Unpaywall (missing email)")
				_report_step("Cannot try Unpaywall (missing email)")
				log(f"[WARN] UNPAYWALL email is required for PubMed DOI {doi} download.")

		else:
			if not saved and unpaywall_email:
				attempts.append("Unpaywall")
				_report_step("Trying Unpaywall")
				pdf_data = download_via_unpaywall(session, doi, unpaywall_email, logger=log)
				if pdf_data:
					pdf_path.write_bytes(pdf_data)
					saved = True
					saved_via = "Unpaywall"
			elif not saved:
				_report_step("Skipping Unpaywall (missing email)")

			if not saved:
				attempts.append("CrossRef")
				_report_step("Trying CrossRef")
				pdf_data = download_via_crossref_open_access(session, doi, logger=log)
				if pdf_data:
					pdf_path.write_bytes(pdf_data)
					saved = True
					saved_via = "CrossRef"

			if not saved:
				attempts.append("DOI landing page")
				_report_step("Trying DOI landing page")
				pdf_data = download_via_doi_landing_page(session, doi, logger=log)
				if pdf_data:
					pdf_path.write_bytes(pdf_data)
					saved = True
					saved_via = "DOI landing page"

		if saved:
			stats["saved"] = int(stats["saved"]) + 1
			_report_step(f"Saved via {saved_via or 'unknown'}", current_value=index)
			saved_items = stats.get("saved_items")
			if isinstance(saved_items, list):
				saved_items.append(
					{
						"doi": doi,
						"source": source,
						"path": str(pdf_path),
					}
				)
		else:
			attempted = ", ".join(attempts) if attempts else "none"
			reason = f"No PDF found via: {attempted}"
			_report_step(f"Failed ({reason})", current_value=index)
			log(f"[FAIL] DOI {doi} | {reason}")
			failed_items = stats.get("failed_items")
			if isinstance(failed_items, list):
				failed_items.append({"doi": doi, "source": source, "reason": reason})
			stats["failed"] = int(stats["failed"]) + 1

		_report_step("Finished", current_value=index)

		time.sleep(0.3)

	return stats


def to_optional_int(value: str) -> Optional[int]:
	cleaned = value.strip()
	if not cleaned:
		return None
	return int(cleaned)


def to_bool(value: str) -> bool:
	return value.strip().lower() in {"y", "yes", "1", "true"}


def normalize_scopus_input(query: str) -> str:
	trimmed = query.strip()
	if re.search(r"\b(TITLE-ABS-KEY|TITLE|ABS|KEY|AUTH|PUBYEAR|DOI|ISSN)\s*\(", trimmed, flags=re.IGNORECASE):
		return trimmed
	return f"TITLE-ABS-KEY({trimmed})"


def run_workflow(
	query: str,
	start_year: Optional[int],
	end_year: Optional[int],
	max_elsevier: int,
	max_pubmed: int,
	final_top_n: Optional[int],
	output_dir: Path,
	skip_download: bool,
	save_csv: bool = True,
	ranking_mode: str = "most_cited",
	elsevier_api_key: Optional[str] = None,
	elsevier_bearer_token: Optional[str] = None,
	ncbi_email: Optional[str] = None,
	ncbi_api_key: Optional[str] = None,
	file_name_mode: str = "doi",
	logger: Optional[Callable[[str], None]] = None,
	progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> Dict[str, object]:
	log = _resolve_logger(logger)

	if elsevier_api_key is None:
		elsevier_api_key = os.getenv("ELSEVIER_API_KEY")
	if elsevier_bearer_token is None:
		elsevier_bearer_token = os.getenv("ELSEVIER_BEARER_TOKEN")
	if ncbi_email is None:
		ncbi_email = os.getenv("NCBI_EMAIL")
	if ncbi_api_key is None:
		ncbi_api_key = os.getenv("NCBI_API_KEY")

	with requests.Session() as session:
		if progress_callback is not None:
			progress_callback(0, 0, "Searching records from Elsevier and PubMed...")

		elsevier_records: List[Dict[str, str]] = []
		if elsevier_api_key:
			elsevier_query = normalize_scopus_input(query)
			elsevier_records = search_elsevier_scopus_dois(
				session,
				api_key=elsevier_api_key,
				query=elsevier_query,
				start_year=start_year,
				end_year=end_year,
				max_results=max_elsevier,
				ranking_mode=ranking_mode,
				bearer_token=elsevier_bearer_token,
				progress_callback=progress_callback,
			)
		else:
			log("[WARN] ELSEVIER_API_KEY not set. Skipping Elsevier fetch.")

		pubmed_fetch_target = max_pubmed
		if (ranking_mode or "").strip().lower() == "most_cited":
			pubmed_fetch_target = min(max(max_pubmed * 6, 250), 600)

		pubmed_records = search_pubmed_dois(
			session,
			query=query,
			start_year=start_year,
			end_year=end_year,
			max_results=pubmed_fetch_target,
			email=ncbi_email,
			api_key=ncbi_api_key,
			progress_callback=progress_callback,
		)
		pubmed_records = enrich_pubmed_citations(
			session,
			pubmed_records,
			logger=log,
			progress_callback=progress_callback,
		)
		pubmed_records = rank_records(pubmed_records, ranking_mode=ranking_mode, limit=max_pubmed)

		unique_records = deduplicate_records(elsevier_records + pubmed_records)
		oa_records = filter_open_access_records(unique_records, logger=log)
		all_records = rank_records(oa_records, ranking_mode=ranking_mode, limit=final_top_n)

		csv_path = output_dir / "doi_list.csv"
		if save_csv:
			save_doi_csv(all_records, csv_path)
			if final_top_n:
				log(
					f"[OK] DOI list saved: {csv_path} "
					f"(selected top {len(all_records)} by cites/rank from {len(unique_records)} unique records)"
				)
			else:
				log(f"[OK] DOI list saved: {csv_path} ({len(all_records)} records)")
		log(f"      Elsevier: {len(elsevier_records)} | PubMed: {len(pubmed_records)}")

		summary = {
			"records_total": len(all_records),
			"records_unique": len(unique_records),
			"records_open_access": len(oa_records),
			"records_elsevier": len(elsevier_records),
			"records_pubmed": len(pubmed_records),
			"records": all_records,
			"saved": 0,
			"skipped": 0,
			"failed": 0,
		}

		if not skip_download:
			if progress_callback is not None:
				progress_callback(0, len(all_records), "Starting download...")
			stats = download_xml_for_records(
				session,
				all_records,
				output_dir=output_dir / "pdf",
				elsevier_api_key=elsevier_api_key,
				elsevier_bearer_token=elsevier_bearer_token,
				ncbi_email=ncbi_email,
				ncbi_api_key=ncbi_api_key,
				file_name_mode=file_name_mode,
				logger=log,
				progress_callback=progress_callback,
			)
			summary.update(stats)
			log(
				"[OK] PDF download finished | "
				f"saved={stats['saved']} skipped={stats['skipped']} failed={stats['failed']}"
			)

		return summary


def run_pdf_download(
	records: List[Dict[str, str]],
	output_dir: Path,
	elsevier_api_key: Optional[str] = None,
	elsevier_bearer_token: Optional[str] = None,
	ncbi_email: Optional[str] = None,
	ncbi_api_key: Optional[str] = None,
	file_name_mode: str = "doi",
	logger: Optional[Callable[[str], None]] = None,
	progress_callback: Optional[Callable[[int, int, str], None]] = None,
	cancel_requested: Optional[Callable[[], bool]] = None,
) -> Dict[str, object]:
	log = _resolve_logger(logger)

	if elsevier_api_key is None:
		elsevier_api_key = os.getenv("ELSEVIER_API_KEY")
	if elsevier_bearer_token is None:
		elsevier_bearer_token = os.getenv("ELSEVIER_BEARER_TOKEN")
	if ncbi_email is None:
		ncbi_email = os.getenv("NCBI_EMAIL")
	if ncbi_api_key is None:
		ncbi_api_key = os.getenv("NCBI_API_KEY")

	with requests.Session() as session:
		stats = download_xml_for_records(
			session,
			records,
			output_dir=output_dir / "pdf",
			elsevier_api_key=elsevier_api_key,
			elsevier_bearer_token=elsevier_bearer_token,
			ncbi_email=ncbi_email,
			ncbi_api_key=ncbi_api_key,
			file_name_mode=file_name_mode,
			logger=log,
			progress_callback=progress_callback,
			cancel_requested=cancel_requested,
		)
		log(
			"[OK] PDF download finished | "
			f"saved={stats['saved']} skipped={stats['skipped']} failed={stats['failed']}"
		)
		return stats


class RetrieveGUI:
	def __init__(self) -> None:
		self.root = tk.Tk()
		self.root.title("pyPublica")
		set_window_icon(self.root)
		self.log_queue: "queue.Queue[Tuple[str, object]]" = queue.Queue()
		self.worker_thread: Optional[threading.Thread] = None
		self.progress_window: Optional[tk.Toplevel] = None
		self.progress_bar: Optional[ttk.Progressbar] = None
		self.progress_label: Optional[ttk.Label] = None
		self.progress_cancel_button: Optional[ttk.Button] = None
		self.download_prompt_window: Optional[tk.Toplevel] = None
		self.download_cancel_event = threading.Event()
		self.last_search_summary: Optional[Dict[str, object]] = None
		self._sort_column: Optional[str] = None
		self._sort_desc: bool = False
		self._table_headers: Dict[str, str] = {}
		self.search_history: List[Dict[str, object]] = []
		self.history_table: Optional[ttk.Treeview] = None
		self.download_button: Optional[ttk.Button] = None
		self.pending_download_request: Optional[Dict[str, object]] = None
		self._pending_download_backup: Optional[Dict[str, object]] = None
		self.cached_search_context: Optional[Dict[str, object]] = None
		self._append_history_entry_next: bool = True
		self._history_replace_index: Optional[int] = None
		self._history_context_index: Optional[int] = None
		self._row_records: Dict[str, Dict[str, str]] = {}
		self._context_menu_item: Optional[str] = None
		self.history_context_menu: Optional[tk.Menu] = None
		self.result_context_menu: Optional[tk.Menu] = None
		self.download_failure_block: Optional[tk.Label] = None

		self.keyword_var = tk.StringVar()
		self.title_var = tk.StringVar()
		self.rank_mode_var = tk.StringVar(value="Most Cited")
		self.reference_style_var = tk.StringVar(value="APA")
		self.start_year_var = tk.StringVar()
		self.end_year_var = tk.StringVar()
		self.max_elsevier_var = tk.StringVar(value="200")
		self.max_pubmed_var = tk.StringVar(value="200")
		self.file_name_mode_var = tk.StringVar(value="DOI")
		self.output_dir_var = tk.StringVar(value=str(DEFAULT_RESULT_CONTENT_DIR))
		self.elsevier_api_key_var = tk.StringVar()
		self.elsevier_bearer_var = tk.StringVar()
		self.ncbi_email_var = tk.StringVar()
		self.ncbi_api_key_var = tk.StringVar()
		self.summary_var = tk.StringVar(value="No results yet.")
		self.download_failure_var = tk.StringVar(value="")

		self._configure_table_styles()
		self._build_menu_bar()
		self._build_ui()
		self._load_credentials()
		self._prompt_credentials_if_needed()
		self._load_search_history()
		self._refresh_history_table()
		self._center_window(self.root, 1220, 780)
		self._poll_log_queue()

	def _center_window(self, window: tk.Misc, width: int, height: int) -> None:
		window.update_idletasks()
		screen_width = window.winfo_screenwidth()
		screen_height = window.winfo_screenheight()
		x_pos = max((screen_width - width) // 2, 0)
		y_pos = max((screen_height - height) // 2, 0)
		window.geometry(f"{width}x{height}+{x_pos}+{y_pos}")

	def _configure_table_styles(self) -> None:
		style = ttk.Style()
		style.configure(
			"Grid.Treeview",
			rowheight=24,
			borderwidth=1,
			relief="solid",
			fieldbackground="#ffffff",
			background="#ffffff",
		)
		style.configure(
			"Grid.Treeview.Heading",
			borderwidth=1,
			relief="solid",
		)

	def _build_menu_bar(self) -> None:
		menubar = tk.Menu(self.root)

		file_menu = tk.Menu(menubar, tearoff=0)
		file_menu.add_command(label="Export Checked Bibliography", command=self.export_checked_bibliography)
		file_menu.add_separator()
		file_menu.add_command(label="New Search", command=self.new_search)
		file_menu.add_command(label="Credential Settings", command=self._open_credentials_dialog)
		file_menu.add_command(label="Reset Credentials", command=self._reset_credentials)
		menubar.add_cascade(label="File", menu=file_menu)

		option_menu = tk.Menu(menubar, tearoff=0)
		for style in ["APA", "Harvard", "IEEE", "MLA", "Vancouver"]:
			option_menu.add_radiobutton(label=style, variable=self.reference_style_var, value=style)
		menubar.add_cascade(label="Reference Style", menu=option_menu)

		self.root.configure(menu=menubar)

	def _load_credentials(self) -> None:
		try:
			self._migrate_legacy_credentials()
			self.elsevier_api_key_var.set(self._keyring_get("elsevier_api_key"))
			self.elsevier_bearer_var.set(self._keyring_get("elsevier_bearer_token"))
			self.ncbi_email_var.set(self._keyring_get("ncbi_email"))
			self.ncbi_api_key_var.set(self._keyring_get("ncbi_api_key"))
		except Exception:
			pass

	def _keyring_get(self, key_name: str) -> str:
		if keyring is None:
			return ""
		try:
			value = keyring.get_password(KEYRING_SERVICE_NAME, key_name)
			return str(value or "").strip()
		except Exception:
			return ""

	def _keyring_set(self, key_name: str, value: str) -> bool:
		if keyring is None:
			return False
		cleaned = str(value or "").strip()
		try:
			if cleaned:
				keyring.set_password(KEYRING_SERVICE_NAME, key_name, cleaned)
			else:
				# Empty values should clear stale secrets from keyring.
				try:
					keyring.delete_password(KEYRING_SERVICE_NAME, key_name)
				except Exception:
					pass
			return True
		except Exception:
			return False

	def _has_saved_credentials(self) -> bool:
		if keyring is None:
			return False
		return any(self._keyring_get(field) for field in KEYRING_FIELDS)

	def _migrate_legacy_credentials(self) -> None:
		if keyring is None:
			return

		for legacy_file in LEGACY_CREDENTIAL_FILES:
			if not legacy_file.exists():
				continue
			try:
				with legacy_file.open("r", encoding="utf-8") as file:
					loaded = json.load(file)
			except Exception:
				continue
			if not isinstance(loaded, dict):
				continue

			for field in KEYRING_FIELDS:
				file_value = str(loaded.get(field, "") or "").strip()
				if not file_value:
					continue
				if self._keyring_get(field):
					continue
				self._keyring_set(field, file_value)

			# Remove plaintext file once all non-empty values are present in keyring.
			all_migrated = True
			for field in KEYRING_FIELDS:
				file_value = str(loaded.get(field, "") or "").strip()
				if file_value and not self._keyring_get(field):
					all_migrated = False
					break
			if all_migrated:
				try:
					legacy_file.unlink()
				except Exception:
					pass

	def _save_credentials(self) -> bool:
		if keyring is None:
			messagebox.showerror(
				"Credential Storage",
				"Secure credential storage requires the keyring package.\nInstall it with: pip install keyring",
			)
			return False

		payload = {
			"elsevier_api_key": self.elsevier_api_key_var.get().strip(),
			"elsevier_bearer_token": self.elsevier_bearer_var.get().strip(),
			"ncbi_email": self.ncbi_email_var.get().strip(),
			"ncbi_api_key": self.ncbi_api_key_var.get().strip(),
		}

		failed_fields: List[str] = []
		for field, value in payload.items():
			if not self._keyring_set(field, value):
				failed_fields.append(field)

		if failed_fields:
			messagebox.showerror(
				"Credential Storage",
				"Failed to save some credentials to keyring: " + ", ".join(failed_fields),
			)
			return False
		return True

	def _reset_credentials(self) -> None:
		if not messagebox.askyesno(
			"Reset Credentials",
			"Delete all saved API credentials from keyring and local credential files?",
		):
			return

		failed_fields: List[str] = []
		if keyring is not None:
			for field in KEYRING_FIELDS:
				try:
					keyring.delete_password(KEYRING_SERVICE_NAME, field)
				except Exception:
					# Missing keys can raise in some backends; only report if key still exists.
					if self._keyring_get(field):
						failed_fields.append(field)

		for legacy_file in LEGACY_CREDENTIAL_FILES:
			try:
				if legacy_file.exists():
					legacy_file.unlink()
			except Exception:
				failed_fields.append(str(legacy_file))

		# Clear current-process env values so fallback won't reuse stale credentials.
		for env_key in CREDENTIAL_ENV_KEYS:
			os.environ.pop(env_key, None)

		# Always clear in-memory values so stale secrets are not reused.
		self.elsevier_api_key_var.set("")
		self.elsevier_bearer_var.set("")
		self.ncbi_email_var.set("")
		self.ncbi_api_key_var.set("")

		if failed_fields:
			messagebox.showwarning(
				"Reset Credentials",
				"Some credentials could not be removed: " + ", ".join(failed_fields),
			)
			return

		open_setup = messagebox.askyesno(
			"Reset Credentials",
			"Saved credentials were removed. Do you want to enter new credentials now?",
		)
		if open_setup:
			self._open_credentials_dialog(first_time=False)

	def _prompt_credentials_if_needed(self) -> None:
		if self._has_saved_credentials():
			return
		self._open_credentials_dialog(first_time=True)

	def _open_credentials_dialog(self, first_time: bool = False) -> None:
		window = tk.Toplevel(self.root)
		window.title("API Setup")
		set_window_icon(window)
		window.transient(self.root)
		window.grab_set()

		frame = ttk.Frame(window, padding=12)
		frame.pack(fill=tk.BOTH, expand=True)

		desc = "Enter API credentials for better retrieval."
		if first_time:
			if keyring is None:
				desc = (
					"First-time setup: enter API credentials. They can be saved securely after keyring is installed. "
					"You can update credentials later from File > Credential Settings."
				)
			else:
				desc = (
					"First-time setup: enter API credentials . "
					"You can update credentials later from File > Credential Settings."
				)
		ttk.Label(frame, text=desc, justify=tk.LEFT).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))

		def _add_entry(row: int, label: str, var: tk.StringVar, show: Optional[str] = None) -> None:
			ttk.Label(frame, text=label).grid(row=row, column=0, sticky="w", pady=4)
			ttk.Entry(frame, textvariable=var, show=show, width=58).grid(row=row, column=1, sticky="ew", pady=4)


		frame.columnconfigure(1, weight=1)
		_add_entry(1, "Elsevier API key", self.elsevier_api_key_var, show="*")
		_add_entry(3, "NCBI email", self.ncbi_email_var, show="*")
		_add_entry(4, "NCBI API key", self.ncbi_api_key_var, show="*")

		button_row = ttk.Frame(frame)
		button_row.grid(row=6, column=0, columnspan=2, sticky="e", pady=(10, 0))

		def _save_and_close() -> None:
			if self._save_credentials():
				window.grab_release()
				window.destroy()

		def _later() -> None:
			window.grab_release()
			window.destroy()

		ttk.Button(button_row, text="Save", command=_save_and_close).pack(side=tk.LEFT)
		ttk.Button(button_row, text="Later", command=_later).pack(side=tk.LEFT, padx=(8, 0))
		self._center_window(window, 620, 260)

	def _build_ui(self) -> None:
		main = ttk.Frame(self.root, padding=12)
		main.pack(fill=tk.BOTH, expand=True)

		main.columnconfigure(0, weight=1)
		main.rowconfigure(2, weight=1)

		history_frame = ttk.LabelFrame(main, text="Search Memory", padding=10)
		history_frame.grid(row=0, column=0, sticky="ew")
		history_frame.columnconfigure(0, weight=1)

		history_columns = ("query", "mode", "year", "papers", "cites", "date")
		self.history_table = ttk.Treeview(history_frame, columns=history_columns, show="headings", height=5, style="Grid.Treeview")
		self.history_table.grid(row=0, column=0, sticky="ew")
		self.history_table.bind("<Double-1>", self._on_history_double_click)
		self.history_table.bind("<Button-3>", self._on_history_right_click)

		history_headers = {
			"query": "Search terms",
			"mode": "Ranker",
			"year": "Year range",
			"papers": "Papers",
			"cites": "Cites",
			"date": "Search date",
		}
		for col, label in history_headers.items():
			self.history_table.heading(col, text=label)

		self.history_table.column("query", width=380, anchor=tk.W)
		self.history_table.column("mode", width=100, anchor=tk.CENTER)
		self.history_table.column("year", width=110, anchor=tk.CENTER)
		self.history_table.column("papers", width=80, anchor=tk.CENTER)
		self.history_table.column("cites", width=80, anchor=tk.CENTER)
		self.history_table.column("date", width=130, anchor=tk.CENTER)

		history_actions = ttk.Frame(history_frame)
		history_actions.grid(row=1, column=0, sticky="e", pady=(8, 0))
		ttk.Button(history_actions, text="New Search", command=self.new_search).pack(side=tk.RIGHT, padx=(0, 8))
		ttk.Button(history_actions, text="Delete Selected", command=self._delete_selected_history_entry).pack(side=tk.RIGHT)

		settings_frame = ttk.LabelFrame(main, text="Search Settings", padding=10)
		settings_frame.grid(row=1, column=0, sticky="ew", pady=(10, 0))
		settings_frame.columnconfigure(1, weight=1)

		self._add_labeled_entry(settings_frame, 0, "Keywords", self.keyword_var)
		self._add_labeled_entry(settings_frame, 1, "Title", self.title_var)

		rank_mode_label = ttk.Label(settings_frame, text="Top ranker")
		rank_mode_label.grid(row=2, column=0, sticky="w", padx=(0, 8), pady=4)
		rank_mode_box = ttk.Combobox(
			settings_frame,
			textvariable=self.rank_mode_var,
			values=["Most Cited", "Newest"],
			state="readonly",
		)
		rank_mode_box.grid(row=2, column=1, sticky="w", pady=4)

		self._add_labeled_entry(settings_frame, 3, "Start year", self.start_year_var)
		self._add_labeled_entry(settings_frame, 4, "End year", self.end_year_var)
		self._add_labeled_entry(settings_frame, 5, "Max Elsevier records", self.max_elsevier_var)
		self._add_labeled_entry(settings_frame, 6, "Max PubMed records", self.max_pubmed_var)

		output_label = ttk.Label(settings_frame, text="Output directory")
		output_label.grid(row=7, column=0, sticky="w", padx=(0, 8), pady=4)
		output_entry = ttk.Entry(settings_frame, textvariable=self.output_dir_var)
		output_entry.grid(row=7, column=1, sticky="ew", pady=4)
		browse_btn = ttk.Button(settings_frame, text="Browse", command=self._browse_output_dir)
		browse_btn.grid(row=7, column=2, sticky="w", padx=(8, 0), pady=4)

		file_name_label = ttk.Label(settings_frame, text="PDF filename")
		file_name_label.grid(row=8, column=0, sticky="w", padx=(0, 8), pady=4)
		file_name_box = ttk.Combobox(
			settings_frame,
			textvariable=self.file_name_mode_var,
			values=["DOI", "Title"],
			state="readonly",
			width=20,
		)
		file_name_box.grid(row=8, column=1, sticky="w", pady=4)

		output_frame = ttk.LabelFrame(main, text="Results", padding=10)
		output_frame.grid(row=2, column=0, sticky="nsew", pady=(10, 0))
		output_frame.columnconfigure(0, weight=1)
		output_frame.rowconfigure(3, weight=1)

		controls = ttk.Frame(output_frame)
		controls.grid(row=0, column=0, sticky="ew", pady=(0, 8))
		controls.columnconfigure(3, weight=1)

		self.run_button = ttk.Button(controls, text="Search", command=self.start_workflow)
		self.run_button.grid(row=0, column=0, sticky="w")

		clear_button = ttk.Button(controls, text="Clear Results", command=self.clear_results)
		clear_button.grid(row=0, column=1, sticky="w", padx=(8, 0))

		self.download_button = ttk.Button(controls, text="Download PDFs", command=self._download_pending)
		self.download_button.grid(row=0, column=2, sticky="w", padx=(8, 0))
		self.download_button.grid_remove()

		self.status_label = ttk.Label(controls, text="Ready")
		self.status_label.grid(row=0, column=3, sticky="e")

		self.download_failure_block = tk.Label(
			output_frame,
			textvariable=self.download_failure_var,
			anchor="w",
			justify=tk.LEFT,
			bg="#fbe4e6",
			fg="#8a1f2b",
			relief="solid",
			borderwidth=1,
			padx=8,
			pady=5,
		)
		self.download_failure_block.grid(row=1, column=0, sticky="ew", pady=(0, 6))
		self.download_failure_block.grid_remove()

		summary_label = ttk.Label(output_frame, textvariable=self.summary_var)
		summary_label.grid(row=2, column=0, sticky="w", pady=(0, 6))

		table_container = ttk.Frame(output_frame)
		table_container.grid(row=3, column=0, sticky="nsew")
		table_container.columnconfigure(0, weight=1)
		table_container.rowconfigure(0, weight=1)

		columns = ("selected", "cites", "per_year", "rank", "authors", "tittle", "year", "publication", "publisher", "type")
		self.results_table = ttk.Treeview(table_container, columns=columns, show="headings", style="Grid.Treeview")
		self.results_table.grid(row=0, column=0, sticky="nsew")
		self.results_table.bind("<Button-1>", self._on_results_left_click, add="+")
		self.results_table.bind("<Button-3>", self._on_results_right_click)
		self.results_table.tag_configure("download_failed", background="#fbe4e6")

		headers = {
			"selected": "Save",
			"cites": "Cites",
			"per_year": "Per Year",
			"rank": "Rank",
			"authors": "Authors",
			"tittle": "Tittle",
			"year": "Year",
			"publication": "Publication",
			"publisher": "Publisher",
			"type": "Type",
		}
		self._table_headers = dict(headers)
		for col, label in headers.items():
			self.results_table.heading(col, text=label, command=lambda c=col: self._sort_results_table(c))

		self.results_table.column("selected", width=52, anchor=tk.CENTER)
		self.results_table.column("cites", width=70, anchor=tk.CENTER)
		self.results_table.column("per_year", width=80, anchor=tk.CENTER)
		self.results_table.column("rank", width=70, anchor=tk.CENTER)
		self.results_table.column("authors", width=230, anchor=tk.W)
		self.results_table.column("tittle", width=320, anchor=tk.W)
		self.results_table.column("year", width=70, anchor=tk.CENTER)
		self.results_table.column("publication", width=220, anchor=tk.W)
		self.results_table.column("publisher", width=180, anchor=tk.W)
		self.results_table.column("type", width=120, anchor=tk.CENTER)

		scroll_y = ttk.Scrollbar(table_container, orient=tk.VERTICAL, command=self.results_table.yview)
		scroll_y.grid(row=0, column=1, sticky="ns")
		scroll_x = ttk.Scrollbar(table_container, orient=tk.HORIZONTAL, command=self.results_table.xview)
		scroll_x.grid(row=1, column=0, sticky="ew")
		self.results_table.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)

		self.result_context_menu = tk.Menu(self.root, tearoff=0)
		self.result_context_menu.add_command(label="Open Journal Online", command=self._open_selected_doi_online)
		self.result_context_menu.add_command(label="Copy Result Citation", command=self._copy_selected_result_citation)
		self.result_context_menu.add_command(label="Download This Paper", command=self._download_selected_result)

		self.history_context_menu = tk.Menu(self.root, tearoff=0)
		self.history_context_menu.add_command(label="Open Search Folder", command=self._open_selected_history_folder)

	def _add_labeled_entry(
		self,
		parent: ttk.LabelFrame,
		row: int,
		label: str,
		variable: tk.StringVar,
		show: Optional[str] = None,
	) -> None:
		lbl = ttk.Label(parent, text=label)
		lbl.grid(row=row, column=0, sticky="w", padx=(0, 8), pady=4)
		entry = ttk.Entry(parent, textvariable=variable, show=show)
		entry.grid(row=row, column=1, columnspan=2, sticky="ew", pady=4)

	def _coerce_table_sort_value(self, column: str, value: str) -> object:
		numeric_columns = {"cites", "per_year", "rank", "year"}
		text = str(value or "").strip()
		if column == "selected":
			return 1 if text == "[x]" else 0
		if column in numeric_columns:
			if not text:
				return float("-inf")
			if column == "year":
				try:
					return int(text)
				except ValueError:
					return -1
			return parse_float(text)
		return text.lower()

	def _refresh_table_header_labels(self) -> None:
		for col, base_label in self._table_headers.items():
			if self._sort_column == col:
				arrow = " ▼" if self._sort_desc else " ▲"
				self.results_table.heading(col, text=f"{base_label}{arrow}", command=lambda c=col: self._sort_results_table(c))
			else:
				self.results_table.heading(col, text=base_label, command=lambda c=col: self._sort_results_table(c))

	def _sort_results_table(self, column: str) -> None:
		if self._sort_column == column:
			self._sort_desc = not self._sort_desc
		else:
			self._sort_column = column
			self._sort_desc = False

		rows: List[Tuple[object, str]] = []
		for item_id in self.results_table.get_children(""):
			cell_value = self.results_table.set(item_id, column)
			rows.append((self._coerce_table_sort_value(column, cell_value), item_id))

		rows.sort(key=lambda pair: pair[0], reverse=self._sort_desc)
		for index, (_, item_id) in enumerate(rows):
			self.results_table.move(item_id, "", index)

		self._refresh_table_header_labels()

	def _on_results_left_click(self, event: tk.Event) -> Optional[str]:
		region = self.results_table.identify("region", event.x, event.y)
		if region != "cell":
			return None
		column = self.results_table.identify_column(event.x)
		row_id = self.results_table.identify_row(event.y)
		if column != "#1" or not row_id:
			return None

		current = self.results_table.set(row_id, "selected")
		self.results_table.set(row_id, "selected", "[ ]" if current == "[x]" else "[x]")
		return "break"

	def _on_results_right_click(self, event: tk.Event) -> None:
		row_id = self.results_table.identify_row(event.y)
		if not row_id or self.result_context_menu is None:
			return
		self.results_table.selection_set(row_id)
		self._context_menu_item = row_id
		try:
			self.result_context_menu.tk_popup(event.x_root, event.y_root)
		finally:
			self.result_context_menu.grab_release()

	def _get_selected_record(self) -> Optional[Dict[str, str]]:
		item_id = self._context_menu_item
		if not item_id:
			selection = self.results_table.selection()
			if selection:
				item_id = selection[0]
		if not item_id:
			return None
		record = self._row_records.get(item_id)
		return dict(record) if record else None

	def _split_author_entries(self, raw_authors: str, source: str) -> List[str]:
		text = re.sub(r"\s+", " ", str(raw_authors or "").strip())
		if not text:
			return []

		if ";" in text:
			return [part.strip() for part in text.split(";") if part.strip()]

		if source == "pubmed":
			return [part.strip() for part in text.split(",") if part.strip()]

		if re.search(r"\band\b", text, flags=re.IGNORECASE):
			parts = re.split(r"\band\b", text, flags=re.IGNORECASE)
			return [part.strip(" ,") for part in parts if part.strip(" ,")]

		if text.count(",") == 1:
			# Common single-author format: "Surname, Given Names"
			return [text]

		return [text]

	def _initials_from_given_names(self, given_names: str) -> str:
		tokens = [tok for tok in re.split(r"\s+", given_names.strip()) if tok]
		initials: List[str] = []
		for token in tokens:
			clean = re.sub(r"[^A-Za-z\-]", "", token)
			if not clean:
				continue
			if "-" in clean:
				h_parts = [h for h in clean.split("-") if h]
				if h_parts:
					initials.append("-".join(f"{h[0].upper()}." for h in h_parts))
			else:
				initials.append(f"{clean[0].upper()}.")
		return " ".join(initials)

	def _format_single_author_surname_initials(self, author_name: str, source: str) -> str:
		name = re.sub(r"\s+", " ", str(author_name or "").strip(" ,"))
		if not name:
			return ""

		surname = ""
		given = ""
		if "," in name:
			surname, given = [part.strip() for part in name.split(",", 1)]
		else:
			parts = name.split(" ")
			if len(parts) == 1:
				return parts[0]
			# PubMed names are produced as "LastName ForeName".
			surname = parts[0]
			given = " ".join(parts[1:])

		initials = self._initials_from_given_names(given)
		if initials:
			return f"{surname}, {initials}"
		return surname

	def _format_authors_surname_initials(self, raw_authors: str, source: str) -> str:
		authors = self._split_author_entries(raw_authors, source)
		formatted = [
			self._format_single_author_surname_initials(author, source)
			for author in authors
			if author.strip()
		]
		cleaned = [author for author in formatted if author]
		if not cleaned:
			return "Unknown author"
		return "; ".join(cleaned)

	def _format_reference(self, record: Dict[str, str], index: int) -> str:
		style = (self.reference_style_var.get() or "APA").strip().upper()
		authors = self._format_authors_surname_initials(
			str(record.get("authors", "") or ""),
			str(record.get("source", "") or "").strip().lower(),
		)
		year = extract_year(str(record.get("date", "") or "")) or "n.d."
		title_raw = str(record.get("title", "") or "").strip() or "Untitled"
		title = re.sub(r"\.+$", "", title_raw)
		journal = str(record.get("journal", "") or "").strip()
		doi = str(record.get("doi", "") or "").strip()
		doi_url = f"https://doi.org/{doi}" if doi else ""

		if style == "IEEE":
			text = f"[{index}] {authors}, \"{title},\" {journal}, {year}. {doi_url}"
		elif style == "HARVARD":
			text = f"{authors} ({year}) {title}. {journal}. Available at: {doi_url}."
		elif style == "MLA":
			text = f"{authors}. \"{title}.\" {journal}, {year}, {doi_url}."
		elif style == "VANCOUVER":
			text = f"{authors}. {title}. {journal}. {year}. Available from: {doi_url}."
		else:
			text = f"{authors} ({year}). {title}. {journal}. {doi_url}"

		return re.sub(r"\s+", " ", text).strip()

	def _open_selected_doi_online(self) -> None:
		record = self._get_selected_record()
		if record is None:
			messagebox.showinfo("Open Journal", "Select a result first.")
			return
		doi = str(record.get("doi", "") or "").strip()
		if not doi:
			messagebox.showinfo("Open Journal", "Selected result has no DOI.")
			return
		webbrowser.open(f"https://doi.org/{quote(doi, safe='/')}")

	def _copy_selected_result_citation(self) -> None:
		record = self._get_selected_record()
		if record is None:
			messagebox.showinfo("Copy Citation", "Select a result first.")
			return
		citation = self._format_reference(record, 1)
		self.root.clipboard_clear()
		self.root.clipboard_append(citation)
		self.root.update_idletasks()
		self.status_label.configure(text="Citation copied")

	def _download_selected_result(self) -> None:
		record = self._get_selected_record()
		if record is None:
			messagebox.showinfo("Download", "Select a result first.")
			return

		request = {
			"records": [record],
			"output_dir": self._resolve_output_dir(),
			"elsevier_api_key": self.elsevier_api_key_var.get().strip() or None,
			"elsevier_bearer_token": self.elsevier_bearer_var.get().strip() or None,
			"ncbi_email": self.ncbi_email_var.get().strip() or None,
			"ncbi_api_key": self.ncbi_api_key_var.get().strip() or None,
			"file_name_mode": normalize_file_name_mode(self.file_name_mode_var.get()),
		}
		self._start_download(
			request,
			preserve_pending_button=True,
			is_individual_download=True,
		)

	def export_checked_bibliography(self) -> None:
		checked_records: List[Dict[str, str]] = []
		for item_id in self.results_table.get_children(""):
			if self.results_table.set(item_id, "selected") != "[x]":
				continue
			record = self._row_records.get(item_id)
			if record:
				checked_records.append(record)

		if not checked_records:
			messagebox.showinfo("Export Bibliography", "No checked rows. Tick rows in Sel column first.")
			return

		initial_name = f"bibliography_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
		output_file = filedialog.asksaveasfilename(
			title="Export Bibliography",
			defaultextension=".txt",
			initialfile=initial_name,
			initialdir=str(self._resolve_output_dir()),
			filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
		)
		if not output_file:
			return

		lines = [self._format_reference(record, idx) for idx, record in enumerate(checked_records, start=1)]
		with Path(output_file).open("w", encoding="utf-8") as file:
			file.write("\n".join(lines))

		self.status_label.configure(text=f"Bibliography exported ({len(lines)})")
		messagebox.showinfo("Export Bibliography", f"Saved {len(lines)} references to:\n{output_file}")

	def _browse_output_dir(self) -> None:
		selected = filedialog.askdirectory(initialdir=str(Path(self.output_dir_var.get() or ".").resolve()))
		if selected:
			target = ensure_output_container(Path(selected))
			self.output_dir_var.set(str(target))

	def _resolve_output_dir(self) -> Path:
		raw = (self.output_dir_var.get() or "").strip()
		if not raw:
			ensure_dir(DEFAULT_RESULT_CONTENT_DIR)
			return DEFAULT_RESULT_CONTENT_DIR
		candidate = Path(raw).expanduser()
		if candidate == DEFAULT_OUTPUT_DIR:
			ensure_dir(DEFAULT_RESULT_CONTENT_DIR)
			return DEFAULT_RESULT_CONTENT_DIR
		if candidate == DEFAULT_RESULT_CONTENT_DIR:
			ensure_dir(DEFAULT_RESULT_CONTENT_DIR)
			return DEFAULT_RESULT_CONTENT_DIR

		# Preserve existing output roots from older runs if they already contain results.
		if (candidate / "doi_list.csv").exists() or (candidate / "pdf").exists():
			ensure_dir(candidate)
			return candidate

		return ensure_output_container(candidate)

	def _build_display_query(self, raw_keywords: str, raw_title: str) -> str:
		display_query = (raw_keywords or "").strip()
		raw_title_clean = (raw_title or "").strip()
		if raw_title_clean:
			display_query = (
				f"{display_query} | title:{raw_title_clean}"
				if display_query
				else f"title:{raw_title_clean}"
			)
		return display_query

	def _safe_search_folder_name(self, value: str) -> str:
		text = (value or "").strip()
		if not text:
			text = "untitled_search"
		# Keep folder readable while removing Windows-invalid path characters.
		text = re.sub(r"[<>:\"/\\|?*\x00-\x1F]", "_", text)
		text = text.rstrip(". ")
		return text or "untitled_search"

	def _ensure_unique_folder(self, folder_path: Path) -> Path:
		if not folder_path.exists():
			return folder_path
		index = 2
		while True:
			candidate = folder_path.parent / f"{folder_path.name}_{index}"
			if not candidate.exists():
				return candidate
			index += 1

	def _resolve_search_output_dir(self, raw_keywords: str, raw_title: str) -> Path:
		base_output_dir = self._resolve_output_dir()

		# If user clicked New Search while still pointing to previous search folder,
		# create the next folder beside it (same parent) rather than nesting deeper.
		if self._append_history_entry_next and isinstance(self.last_search_summary, dict):
			last_output = str(self.last_search_summary.get("output_dir", "") or "").strip()
			if last_output:
				try:
					if base_output_dir.resolve() == Path(last_output).resolve():
						base_output_dir = Path(last_output).parent
				except Exception:
					pass

		display_query = self._build_display_query(raw_keywords, raw_title)
		folder_name = self._safe_search_folder_name(display_query)
		target_dir = base_output_dir / folder_name

		if self._append_history_entry_next:
			target_dir = self._ensure_unique_folder(target_dir)

		return target_dir

	def _load_search_history(self) -> None:
		try:
			if not SEARCH_HISTORY_FILE.exists():
				self.search_history = []
				return
			with SEARCH_HISTORY_FILE.open("r", encoding="utf-8") as file:
				loaded = json.load(file)
			if isinstance(loaded, list):
				self.search_history = loaded[:300]
			else:
				self.search_history = []
		except Exception:
			self.search_history = []

	def _save_search_history(self) -> None:
		try:
			ensure_dir(SEARCH_HISTORY_FILE.parent)
			with SEARCH_HISTORY_FILE.open("w", encoding="utf-8") as file:
				json.dump(self.search_history[:300], file, ensure_ascii=False, indent=2)
		except Exception:
			pass

	def _refresh_history_table(self) -> None:
		if self.history_table is None:
			return
		self._history_context_index = None
		for row in self.history_table.get_children():
			self.history_table.delete(row)
		for idx, item in enumerate(self.search_history[:100]):
			self.history_table.insert(
				"",
				tk.END,
				iid=str(idx),
				values=(
					item.get("query", ""),
					item.get("mode", ""),
					item.get("year", ""),
					item.get("papers", ""),
					item.get("cites", ""),
					item.get("date", ""),
				),
			)

	def _on_history_double_click(self, _event: object) -> None:
		if self.history_table is None:
			return
		selection = self.history_table.selection()
		if not selection:
			return
		try:
			index = int(selection[0])
		except ValueError:
			return
		if index < 0 or index >= len(self.search_history):
			return

		self._append_history_entry_next = False
		self._history_replace_index = index

		entry = self.search_history[index]
		self.keyword_var.set(str(entry.get("raw_keywords", "")))
		self.title_var.set(str(entry.get("raw_title", "")))
		self.rank_mode_var.set("Newest" if str(entry.get("mode", "")).lower() == "newest" else "Most Cited")
		self.start_year_var.set(str(entry.get("start_year", "") or ""))
		self.end_year_var.set(str(entry.get("end_year", "") or ""))
		max_elsevier = int(entry.get("max_elsevier", 200) or 200)
		max_pubmed = int(entry.get("max_pubmed", 200) or 200)
		self.max_elsevier_var.set(str(max_elsevier if max_elsevier > 0 else 200))
		self.max_pubmed_var.set(str(max_pubmed if max_pubmed > 0 else 200))
		file_name_mode = normalize_file_name_mode(str(entry.get("file_name_mode", "doi") or "doi"))
		self.file_name_mode_var.set("Title" if file_name_mode == "title" else "DOI")

		records = entry.get("records", [])
		output_dir_value = str(entry.get("output_dir", "") or "")
		if output_dir_value:
			self.output_dir_var.set(output_dir_value)

		if isinstance(records, list) and records:
			self.clear_results()
			for idx, record in enumerate(records, start=1):
				if isinstance(record, dict):
					self._append_result_row(record, idx)

			records_elsevier = len([rec for rec in records if isinstance(rec, dict) and rec.get("source") == "elsevier"])
			records_pubmed = len([rec for rec in records if isinstance(rec, dict) and rec.get("source") == "pubmed"])
			self.summary_var.set(
				f"Loaded from memory: {len(records)} records | "
				f"Elsevier: {records_elsevier} | PubMed: {records_pubmed}"
			)

			self.cached_search_context = {
				"records": list(records),
				"raw_keywords": entry.get("raw_keywords", ""),
				"raw_title": entry.get("raw_title", ""),
				"start_year": entry.get("start_year"),
				"end_year": entry.get("end_year"),
			}

			self.last_search_summary = {
				"records_total": len(records),
				"records_unique": len(records),
				"records_elsevier": records_elsevier,
				"records_pubmed": records_pubmed,
				"records": records,
			}

			self._set_pending_download(
				{
					"records": records,
					"output_dir": self._resolve_output_dir(),
					"elsevier_api_key": self.elsevier_api_key_var.get().strip() or None,
					"elsevier_bearer_token": self.elsevier_bearer_var.get().strip() or None,
					"ncbi_email": self.ncbi_email_var.get().strip() or None,
					"ncbi_api_key": self.ncbi_api_key_var.get().strip() or None,
					"file_name_mode": normalize_file_name_mode(self.file_name_mode_var.get()),
				},
				visible=True,
			)
			self.status_label.configure(text="Loaded from memory")
			return

		self.start_workflow()

	def _on_history_right_click(self, event: tk.Event) -> None:
		if self.history_table is None or self.history_context_menu is None:
			return
		row_id = self.history_table.identify_row(event.y)
		if not row_id:
			return
		self.history_table.selection_set(row_id)
		try:
			self._history_context_index = int(row_id)
		except ValueError:
			self._history_context_index = None
		try:
			self.history_context_menu.tk_popup(event.x_root, event.y_root)
		finally:
			self.history_context_menu.grab_release()

	def _open_selected_history_folder(self) -> None:
		index = self._history_context_index
		if index is None and self.history_table is not None:
			selection = self.history_table.selection()
			if selection:
				try:
					index = int(selection[0])
				except ValueError:
					index = None

		if index is None or index < 0 or index >= len(self.search_history):
			messagebox.showinfo("Search Memory", "Select a memory row first.")
			return

		entry = self.search_history[index]
		output_dir = str(entry.get("output_dir", "") or "").strip()
		if not output_dir:
			messagebox.showinfo("Search Memory", "Selected memory has no saved folder.")
			return

		self._open_system_path(Path(output_dir))

	def _delete_selected_history_entry(self) -> None:
		if self.history_table is None:
			return
		selection = self.history_table.selection()
		if not selection:
			messagebox.showinfo("Search Memory", "Select a history row first.")
			return
		if not messagebox.askyesno("Delete history", "Delete selected history entry?"):
			return

		indices = sorted([int(item) for item in selection if str(item).isdigit()], reverse=True)
		for index in indices:
			if 0 <= index < len(self.search_history):
				self.search_history.pop(index)
		if not self.search_history:
			self._append_history_entry_next = True
			self._history_replace_index = None
		else:
			self._history_replace_index = None
		self._save_search_history()
		self._refresh_history_table()

	def _set_pending_download(self, request: Optional[Dict[str, object]], visible: bool) -> None:
		self.pending_download_request = request
		if self.download_button is None:
			return
		if visible and request is not None:
			self.download_button.configure(state=tk.NORMAL)
			self.download_button.grid()
		else:
			self.download_button.configure(state=tk.DISABLED)
			self.download_button.grid_remove()

	def _download_pending(self) -> None:
		if self.pending_download_request is None:
			return
		self._start_download(self.pending_download_request)

	def _can_refine_from_cache(
		self,
		raw_keywords: str,
		raw_title: str,
		start_year: Optional[int],
		end_year: Optional[int],
	) -> bool:
		if self.cached_search_context is None:
			return False
		prev = self.cached_search_context
		prev_records = prev.get("records", [])
		if not isinstance(prev_records, list) or not prev_records:
			return False

		prev_terms = set(parse_query_terms(str(prev.get("raw_keywords", ""))))
		new_terms = set(parse_query_terms(raw_keywords))
		if prev_terms and not prev_terms.issubset(new_terms):
			return False

		prev_title = str(prev.get("raw_title", "") or "").strip().lower()
		new_title = raw_title.strip().lower()
		if prev_title and (not new_title or prev_title not in new_title):
			return False

		if not is_year_range_subset(
			prev.get("start_year"),
			prev.get("end_year"),
			start_year,
			end_year,
		):
			return False

		return True

	def _build_summary_from_cached_records(
		self,
		records: List[Dict[str, str]],
		ranking_mode: str,
		query: str,
		raw_keywords: str,
		raw_title: str,
		start_year: Optional[int],
		end_year: Optional[int],
		file_name_mode: str,
	) -> Dict[str, object]:
		oa_records = filter_open_access_records(records)
		ranked = rank_records(oa_records, ranking_mode=ranking_mode, limit=None)
		records_elsevier = len([rec for rec in ranked if rec.get("source") == "elsevier"])
		records_pubmed = len([rec for rec in ranked if rec.get("source") == "pubmed"])
		return {
			"records_total": len(ranked),
			"records_unique": len(records),
			"records_open_access": len(oa_records),
			"records_elsevier": records_elsevier,
			"records_pubmed": records_pubmed,
			"records": ranked,
			"saved": 0,
			"skipped": 0,
			"failed": 0,
			"query": query,
			"raw_keywords": raw_keywords,
			"raw_title": raw_title,
			"ranking_mode": ranking_mode,
			"start_year": start_year,
			"end_year": end_year,
			"file_name_mode": normalize_file_name_mode(file_name_mode),
		}

	def _add_search_history_entry(self, summary: Dict[str, object]) -> None:
		records = summary.get("records", [])
		total_cites = int(sum(parse_float(rec.get("cites", "")) for rec in records if isinstance(rec, dict)))
		start_year = summary.get("start_year")
		end_year = summary.get("end_year")
		year_text = ""
		if start_year and end_year:
			year_text = f"{start_year}-{end_year}"
		elif start_year:
			year_text = f"{start_year}+"
		elif end_year:
			year_text = f"<= {end_year}"

		raw_keywords = str(summary.get("raw_keywords", "") or "")
		raw_title = str(summary.get("raw_title", "") or "")
		display_query = self._build_display_query(raw_keywords, raw_title)

		entry = {
			"query": display_query,
			"raw_keywords": raw_keywords,
			"raw_title": raw_title,
			"mode": "Newest" if str(summary.get("ranking_mode", "most_cited")).lower() == "newest" else "Most Cited",
			"file_name_mode": normalize_file_name_mode(str(summary.get("file_name_mode", "doi") or "doi")),
			"start_year": start_year,
			"end_year": end_year,
			"max_elsevier": int(summary.get("max_elsevier", 0) or 0),
			"max_pubmed": int(summary.get("max_pubmed", 0) or 0),
			"output_dir": str(summary.get("output_dir", "") or ""),
			"records": [
				{
					"rank": rec.get("rank", ""),
					"doi": rec.get("doi", ""),
					"source": rec.get("source", ""),
					"title": rec.get("title", ""),
					"date": rec.get("date", ""),
					"journal": rec.get("journal", ""),
					"authors": rec.get("authors", ""),
					"publisher": rec.get("publisher", ""),
					"cites": rec.get("cites", ""),
					"type": rec.get("type", ""),
					"id": rec.get("id", ""),
				}
				for rec in records[:500]
				if isinstance(rec, dict)
			],
			"year": year_text,
			"papers": int(summary.get("records_total", 0) or 0),
			"cites": total_cites,
			"date": datetime.now().strftime("%Y-%m-%d %H:%M"),
		}

		replace_index = self._history_replace_index
		if replace_index is None or replace_index < 0 or replace_index >= len(self.search_history):
			replace_index = 0

		if self._append_history_entry_next or not self.search_history:
			# Add a fresh memory entry only when user starts a new search context.
			if self.search_history and self.search_history[0].get("query") == entry["query"] and self.search_history[0].get("year") == entry["year"] and self.search_history[0].get("mode") == entry["mode"]:
				self.search_history[0] = entry
			else:
				self.search_history.insert(0, entry)
			replace_index = 0
		else:
			# Continuing search updates existing memory instead of appending.
			self.search_history[replace_index] = entry

		self.search_history = self.search_history[:300]
		if self.search_history:
			self._history_replace_index = min(replace_index, len(self.search_history) - 1)
		else:
			self._history_replace_index = None
		self._append_history_entry_next = False
		self._save_search_history()
		self._refresh_history_table()

	def _set_running(self, running: bool, status_text: Optional[str] = None) -> None:
		self.run_button.configure(state=tk.DISABLED if running else tk.NORMAL)
		if status_text is not None:
			self.status_label.configure(text=status_text)
		else:
			self.status_label.configure(text="Running..." if running else "Ready")

	def _show_download_failure_block(self, message: str) -> None:
		if self.download_failure_block is None:
			return
		self.download_failure_var.set(message)
		self.download_failure_block.grid()

	def _hide_download_failure_block(self) -> None:
		if self.download_failure_block is None:
			return
		self.download_failure_var.set("")
		self.download_failure_block.grid_remove()

	def _clear_failed_download_row_marks(self) -> None:
		if not hasattr(self, "results_table"):
			return
		for item_id in self.results_table.get_children(""):
			tags = tuple(tag for tag in self.results_table.item(item_id, "tags") if tag != "download_failed")
			self.results_table.item(item_id, tags=tags)

	def _mark_failed_download_rows(self, failed_items: object) -> None:
		if not isinstance(failed_items, list):
			self._clear_failed_download_row_marks()
			return

		failed_pairs: Set[Tuple[str, str]] = set()
		failed_dois: Set[str] = set()
		for item in failed_items:
			if not isinstance(item, dict):
				continue
			doi = str(item.get("doi", "") or "").strip().lower()
			source = str(item.get("source", "") or "").strip().lower()
			if not doi:
				continue
			failed_dois.add(doi)
			if source:
				failed_pairs.add((doi, source))

		for item_id, record in self._row_records.items():
			doi = str(record.get("doi", "") or "").strip().lower()
			source = str(record.get("source", "") or "").strip().lower()
			is_failed = doi in failed_dois or (doi, source) in failed_pairs
			tags = [tag for tag in self.results_table.item(item_id, "tags") if tag != "download_failed"]
			if is_failed:
				tags.append("download_failed")
			self.results_table.item(item_id, tags=tuple(tags))

	def _open_progress_window(self, title: str = "Progress", initial_message: str = "Working...") -> None:
		if self.progress_window is not None and self.progress_window.winfo_exists():
			if self.progress_window.title() != title:
				self.progress_window.title(title)
			if self.progress_label is not None:
				self.progress_label.configure(text=initial_message)
			return

		window = tk.Toplevel(self.root)
		window.title(title)
		window.transient(self.root)
		window.grab_set()

		frame = ttk.Frame(window, padding=12)
		frame.pack(fill=tk.BOTH, expand=True)

		self.progress_label = ttk.Label(frame, text=initial_message)
		self.progress_label.pack(anchor="w", pady=(0, 8))

		self.progress_bar = ttk.Progressbar(frame, mode="indeterminate", length=420)
		self.progress_bar.pack(fill=tk.X)
		self.progress_bar.start(8)

		if title == "Download Progress":
			self.progress_cancel_button = ttk.Button(frame, text="Cancel Download", command=self._request_cancel_download)
			self.progress_cancel_button.pack(anchor="e", pady=(10, 0))
			window.protocol("WM_DELETE_WINDOW", self._request_cancel_download)
		else:
			self.progress_cancel_button = None

		self.progress_window = window
		self._center_window(window, 460, 140)

	def _request_cancel_download(self) -> None:
		self.download_cancel_event.set()
		if self.progress_label is not None:
			self.progress_label.configure(text="Cancelling download... please wait")
		if self.progress_cancel_button is not None:
			self.progress_cancel_button.configure(state=tk.DISABLED, text="Cancelling...")
		self.status_label.configure(text="Cancelling download...")

	def _close_progress_window(self) -> None:
		if self.progress_bar is not None:
			self.progress_bar.stop()
		if self.progress_window is not None and self.progress_window.winfo_exists():
			self.progress_window.grab_release()
			self.progress_window.destroy()
		self.progress_window = None
		self.progress_bar = None
		self.progress_label = None
		self.progress_cancel_button = None

	def _update_progress_window(self, current: int, total: int, message: str) -> None:
		if self.progress_window is None or not self.progress_window.winfo_exists():
			return
		if self.progress_label is not None:
			self.progress_label.configure(text=message)

		if self.progress_bar is None:
			return

		if total > 0:
			if str(self.progress_bar.cget("mode")) != "determinate":
				self.progress_bar.stop()
				self.progress_bar.configure(mode="determinate")
			self.progress_bar.configure(maximum=total, value=current)
		else:
			if str(self.progress_bar.cget("mode")) != "indeterminate":
				self.progress_bar.configure(mode="indeterminate")
				self.progress_bar.start(8)

	def _close_download_prompt(self) -> None:
		if self.download_prompt_window is not None and self.download_prompt_window.winfo_exists():
			self.download_prompt_window.grab_release()
			self.download_prompt_window.destroy()
		self.download_prompt_window = None

	def _open_download_prompt(self, request: Dict[str, object]) -> None:
		records = request.get("records", [])
		record_count = len(records) if isinstance(records, list) else 0
		if record_count == 0:
			messagebox.showinfo("No records", "No records found to download.")
			return
		self.pending_download_request = request

		self._close_download_prompt()

		window = tk.Toplevel(self.root)
		window.title("Download PDFs")
		window.transient(self.root)
		window.grab_set()

		frame = ttk.Frame(window, padding=14)
		frame.pack(fill=tk.BOTH, expand=True)

		msg = (
			f"Found {record_count} records.\n"
			"Do you want to download PDF files now?"
		)
		ttk.Label(frame, text=msg, justify=tk.LEFT).pack(anchor="w")

		button_row = ttk.Frame(frame)
		button_row.pack(anchor="e", pady=(14, 0))

		ttk.Button(button_row, text="Download PDFs", command=lambda: self._start_download(request)).pack(
			side=tk.LEFT
		)
		ttk.Button(button_row, text="Not now", command=self._skip_download).pack(side=tk.LEFT, padx=(8, 0))

		window.protocol("WM_DELETE_WINDOW", self._skip_download)
		self.download_prompt_window = window
		self._center_window(window, 420, 170)

	def _skip_download(self) -> None:
		self._close_download_prompt()
		self._set_pending_download(self.pending_download_request, visible=True)
		self._set_running(False, "Ready")

	def _start_download(
		self,
		request: Dict[str, object],
		preserve_pending_button: bool = False,
		is_individual_download: bool = False,
	) -> None:
		self.download_cancel_event.clear()
		self._close_download_prompt()
		self._hide_download_failure_block()
		self._clear_failed_download_row_marks()
		if preserve_pending_button and self.pending_download_request is not None:
			self._pending_download_backup = dict(self.pending_download_request)
		else:
			self._pending_download_backup = None
		self._set_pending_download(None, visible=False)
		self._set_running(True, "Downloading PDFs...")
		self._open_progress_window("Download Progress", "Preparing download...")

		worker_kwargs = dict(request)
		worker_kwargs["__is_individual_download"] = is_individual_download

		self.worker_thread = threading.Thread(
			target=self._run_download_worker,
			kwargs=worker_kwargs,
			daemon=True,
		)
		self.worker_thread.start()

	def _open_system_path(self, target_path: Path) -> None:
		if not target_path.exists():
			messagebox.showwarning("Open Path", f"Path not found:\n{target_path}")
			return
		if os.name == "nt":
			os.startfile(str(target_path))
			return
		webbrowser.open(target_path.resolve().as_uri())

	def _show_open_location_prompt(self, kind: str, path_text: str) -> None:
		target = Path(path_text)
		window = tk.Toplevel(self.root)
		window.title("Download Complete")
		window.transient(self.root)
		window.grab_set()

		frame = ttk.Frame(window, padding=12)
		frame.pack(fill=tk.BOTH, expand=True)

		if kind == "file":
			msg = f"Individual download succeeded.\nOpen downloaded file?\n\n{target}"
		else:
			msg = f"Batch download finished.\nOpen download folder?\n\n{target}"
		ttk.Label(frame, text=msg, justify=tk.LEFT, wraplength=520).pack(anchor="w")

		button_row = ttk.Frame(frame)
		button_row.pack(anchor="e", pady=(12, 0))

		if kind == "file":
			ttk.Button(
				button_row,
				text="Open File",
				command=lambda: (self._open_system_path(target), window.destroy()),
			).pack(side=tk.LEFT)
			ttk.Button(
				button_row,
				text="Open Folder",
				command=lambda: (self._open_system_path(target.parent), window.destroy()),
			).pack(side=tk.LEFT, padx=(8, 0))
		else:
			ttk.Button(
				button_row,
				text="Open Folder",
				command=lambda: (self._open_system_path(target), window.destroy()),
			).pack(side=tk.LEFT)

		ttk.Button(button_row, text="Close", command=window.destroy).pack(side=tk.LEFT, padx=(8, 0))
		self._center_window(window, 640, 220)

	def clear_results(self) -> None:
		for item in self.results_table.get_children():
			self.results_table.delete(item)
		self._row_records.clear()
		self._context_menu_item = None
		self._clear_failed_download_row_marks()
		self.summary_var.set("No results yet.")

	def new_search(self) -> None:
		self.keyword_var.set("")
		self.title_var.set("")
		self.start_year_var.set("")
		self.end_year_var.set("")
		self.clear_results()
		self._close_download_prompt()
		self._close_progress_window()
		self._hide_download_failure_block()
		self._clear_failed_download_row_marks()
		self._set_pending_download(None, visible=False)
		self.cached_search_context = None
		self.last_search_summary = None
		self._sort_column = None
		self._sort_desc = False
		self._append_history_entry_next = True
		self._history_replace_index = None
		self._refresh_table_header_labels()
		self._set_running(False, "Ready")

	def _append_result_row(self, record: Dict[str, str], index: int) -> None:
		year = extract_year(record.get("date", ""))
		cites = str(record.get("cites", "") or "")
		per_year = calculate_per_year(cites, year)
		rank_value = record.get("rank", "") or str(index)
		type_value = normalize_publication_type(record.get("type", ""), record.get("source", ""))
		item_id = self.results_table.insert(
			"",
			tk.END,
			values=(
				"[ ]",
				cites,
				per_year,
				rank_value,
				record.get("authors", ""),
				record.get("title", ""),
				year,
				record.get("journal", ""),
				record.get("publisher", ""),
				type_value,
			),
		)
		self._row_records[item_id] = dict(record)

	def _populate_results(self, records: List[Dict[str, str]]) -> None:
		self.clear_results()
		for index, record in enumerate(records, start=1):
			self._append_result_row(record, index)

	def _parse_optional_year(self, raw: str, field_name: str) -> Optional[int]:
		value = raw.strip()
		if not value:
			return None
		try:
			return int(value)
		except ValueError as error:
			raise ValueError(f"{field_name} must be an integer.") from error

	def start_workflow(self) -> None:
		raw_keywords = self.keyword_var.get().strip()
		raw_title = self.title_var.get().strip()
		if not raw_keywords and not raw_title:
			messagebox.showerror("Validation error", "At least one of Keywords or Title is required.")
			return

		try:
			query_parts: List[str] = []
			if raw_keywords:
				query_parts.append(build_keyword_query(raw_keywords, "AUTO"))
			if raw_title:
				title_safe = raw_title.replace('"', " ").strip()
				query_parts.append(f'"{title_safe}"')
			query = query_parts[0] if len(query_parts) == 1 else f"({' AND '.join(query_parts)})"
		except ValueError as error:
			messagebox.showerror("Validation error", str(error))
			return

		try:
			start_year = self._parse_optional_year(self.start_year_var.get(), "Start year")
			end_year = self._parse_optional_year(self.end_year_var.get(), "End year")
			max_elsevier = int(self.max_elsevier_var.get().strip() or "200")
			max_pubmed = int(self.max_pubmed_var.get().strip() or "200")
			final_top_n = None
			if max_elsevier <= 0 or max_pubmed <= 0:
				raise ValueError("Max record values must be greater than zero.")
		except ValueError as error:
			messagebox.showerror("Validation error", str(error))
			return

		self._close_download_prompt()
		self._close_progress_window()
		self._hide_download_failure_block()
		self._clear_failed_download_row_marks()
		self._set_pending_download(None, visible=False)
		self.last_search_summary = None
		self._set_running(True, "Searching...")
		self.summary_var.set("Searching records...")
		self._open_progress_window("Search Progress", "Preparing search...")

		output_dir = self._resolve_output_dir()
		self.output_dir_var.set(str(output_dir))
		rank_mode_label = (self.rank_mode_var.get() or "Most Cited").strip().lower()
		ranking_mode = "newest" if rank_mode_label == "newest" else "most_cited"
		file_name_mode = normalize_file_name_mode(self.file_name_mode_var.get())
		elsevier_api_key = self.elsevier_api_key_var.get().strip() or None
		elsevier_bearer = self.elsevier_bearer_var.get().strip() or None
		ncbi_email = self.ncbi_email_var.get().strip() or None
		ncbi_api_key = self.ncbi_api_key_var.get().strip() or None
		use_cached_refine = self._can_refine_from_cache(raw_keywords, raw_title, start_year, end_year)
		cached_records = []
		if use_cached_refine and self.cached_search_context is not None:
			cached_records = list(self.cached_search_context.get("records", []))

		self.worker_thread = threading.Thread(
			target=self._run_search_worker,
			kwargs={
				"query": query,
				"raw_keywords": raw_keywords,
				"raw_title": raw_title,
				"start_year": start_year,
				"end_year": end_year,
				"max_elsevier": max_elsevier,
				"max_pubmed": max_pubmed,
				"final_top_n": final_top_n,
				"ranking_mode": ranking_mode,
				"use_cached_refine": use_cached_refine,
				"cached_records": cached_records,
				"output_dir": output_dir,
				"file_name_mode": file_name_mode,
				"elsevier_api_key": elsevier_api_key,
				"elsevier_bearer_token": elsevier_bearer,
				"ncbi_email": ncbi_email,
				"ncbi_api_key": ncbi_api_key,
			},
			daemon=True,
		)
		self.worker_thread.start()

	def _run_search_worker(self, **kwargs) -> None:
		def log_callback(message: str) -> None:
			_ = message

		def progress_callback(current: int, total: int, message: str) -> None:
			self.log_queue.put(("progress", (current, total, message)))

		try:
			if kwargs.get("use_cached_refine"):
				progress_callback(0, 1, "Refining from previous search cache...")
				prev_ctx = self.cached_search_context or {}
				prev_keywords = str(prev_ctx.get("raw_keywords", "") or "").strip().lower()
				prev_title = str(prev_ctx.get("raw_title", "") or "").strip().lower()
				new_keywords = str(kwargs.get("raw_keywords", "") or "").strip().lower()
				new_title = str(kwargs.get("raw_title", "") or "").strip().lower()
				same_query_context = prev_keywords == new_keywords and prev_title == new_title

				refine_terms = [] if same_query_context else parse_query_terms(kwargs.get("raw_keywords", ""))
				refine_title = "" if same_query_context else kwargs.get("raw_title", "")
				filtered_records = filter_records_locally(
					kwargs.get("cached_records", []),
					refine_terms,
					refine_title,
					kwargs.get("start_year"),
					kwargs.get("end_year"),
				)
				if filtered_records:
					summary = self._build_summary_from_cached_records(
						records=filtered_records,
						ranking_mode=kwargs["ranking_mode"],
						query=kwargs["query"],
						raw_keywords=kwargs.get("raw_keywords", ""),
						raw_title=kwargs.get("raw_title", ""),
						start_year=kwargs.get("start_year"),
						end_year=kwargs.get("end_year"),
						file_name_mode=kwargs.get("file_name_mode", "doi"),
					)
					progress_callback(1, 1, "Cache refinement complete")
				else:
					progress_callback(0, 0, "No cache match, running full source search...")
					summary = run_workflow(
						query=kwargs["query"],
						start_year=kwargs["start_year"],
						end_year=kwargs["end_year"],
						max_elsevier=kwargs["max_elsevier"],
						max_pubmed=kwargs["max_pubmed"],
						final_top_n=kwargs["final_top_n"],
						output_dir=kwargs["output_dir"],
						skip_download=True,
						save_csv=False,
						ranking_mode=kwargs["ranking_mode"],
						elsevier_api_key=kwargs["elsevier_api_key"],
						elsevier_bearer_token=kwargs["elsevier_bearer_token"],
						ncbi_email=kwargs["ncbi_email"],
						ncbi_api_key=kwargs["ncbi_api_key"],
						file_name_mode=kwargs.get("file_name_mode", "doi"),
						logger=log_callback,
						progress_callback=progress_callback,
					)
			else:
				summary = run_workflow(
					query=kwargs["query"],
					start_year=kwargs["start_year"],
					end_year=kwargs["end_year"],
					max_elsevier=kwargs["max_elsevier"],
					max_pubmed=kwargs["max_pubmed"],
					final_top_n=kwargs["final_top_n"],
					output_dir=kwargs["output_dir"],
					skip_download=True,
					save_csv=False,
					ranking_mode=kwargs["ranking_mode"],
					elsevier_api_key=kwargs["elsevier_api_key"],
					elsevier_bearer_token=kwargs["elsevier_bearer_token"],
					ncbi_email=kwargs["ncbi_email"],
					ncbi_api_key=kwargs["ncbi_api_key"],
					file_name_mode=kwargs.get("file_name_mode", "doi"),
					logger=log_callback,
					progress_callback=progress_callback,
				)
			summary["query"] = kwargs["query"]
			summary["raw_keywords"] = kwargs.get("raw_keywords", "")
			summary["raw_title"] = kwargs.get("raw_title", "")
			summary["ranking_mode"] = kwargs["ranking_mode"]
			summary["start_year"] = kwargs["start_year"]
			summary["end_year"] = kwargs["end_year"]
			summary["max_elsevier"] = kwargs["max_elsevier"]
			summary["max_pubmed"] = kwargs["max_pubmed"]
			summary["output_dir"] = str(kwargs["output_dir"])
			summary["file_name_mode"] = normalize_file_name_mode(str(kwargs.get("file_name_mode", "doi") or "doi"))

			records = summary.get("records", [])
			total = len(records)
			self.log_queue.put(("table_reset", total))
			for index, record in enumerate(records, start=1):
				self.log_queue.put(("record_row", (index, total, record)))

			csv_path = kwargs["output_dir"] / "doi_list.csv"
			save_doi_csv(records, csv_path)
			self.log_queue.put(("csv_saved", str(csv_path)))

			self.log_queue.put(("search_summary", summary))

			download_request = {
				"records": records,
				"output_dir": kwargs["output_dir"],
				"elsevier_api_key": kwargs["elsevier_api_key"],
				"elsevier_bearer_token": kwargs["elsevier_bearer_token"],
				"ncbi_email": kwargs["ncbi_email"],
				"ncbi_api_key": kwargs["ncbi_api_key"],
				"file_name_mode": normalize_file_name_mode(str(kwargs.get("file_name_mode", "doi") or "doi")),
			}
			self.log_queue.put(("ask_download", download_request))
		except Exception:
			self.log_queue.put(("debug", traceback.format_exc()))
			self.log_queue.put(("error", "Search failed. Check credentials or query."))
		finally:
			self.log_queue.put(("state", "idle_search"))

	def _run_download_worker(self, **kwargs) -> None:
		def log_callback(message: str) -> None:
			_ = message

		def progress_callback(current: int, total: int, message: str) -> None:
			self.log_queue.put(("progress", (current, total, message)))

		try:
			is_individual_download = bool(kwargs.pop("__is_individual_download", False))
			stats = run_pdf_download(
				records=kwargs["records"],
				output_dir=kwargs["output_dir"],
				elsevier_api_key=kwargs["elsevier_api_key"],
				elsevier_bearer_token=kwargs["elsevier_bearer_token"],
				ncbi_email=kwargs["ncbi_email"],
				ncbi_api_key=kwargs["ncbi_api_key"],
				file_name_mode=normalize_file_name_mode(str(kwargs.get("file_name_mode", "doi") or "doi")),
				logger=log_callback,
				progress_callback=progress_callback,
				cancel_requested=self.download_cancel_event.is_set,
			)
			self.log_queue.put(("download_summary", stats))
			failed_count = int(stats.get("failed", 0) or 0)
			saved_count = int(stats.get("saved", 0) or 0)
			skipped_count = int(stats.get("skipped", 0) or 0)
			cancelled = bool(stats.get("cancelled", False))
			failed_items = stats.get("failed_items", [])
			saved_items = stats.get("saved_items", [])
			failed_preview = ""
			if isinstance(failed_items, list) and failed_items:
				preview_lines: List[str] = []
				for item in failed_items[:5]:
					if not isinstance(item, dict):
						continue
					preview_lines.append(f"- {item.get('doi', '')}: {item.get('reason', '')}")
				if preview_lines:
					failed_preview = "\n\nFailed examples:\n" + "\n".join(preview_lines)

			if cancelled:
				self.log_queue.put((
					"warn",
					f"Download cancelled by user. saved={saved_count}, skipped={skipped_count}, failed={failed_count}.",
				))
			elif failed_count > 0 and saved_count == 0:
				self.log_queue.put((
					"error",
					f"Download failed for all files ({failed_count} failed).{failed_preview}",
				))
			elif failed_count > 0:
				self.log_queue.put((
					"warn",
					f"Download completed with failures: saved={saved_count}, failed={failed_count}.{failed_preview}",
				))
			else:
				self.log_queue.put(("ok", "PDF download completed."))

			if (saved_count > 0 or skipped_count > 0) and not cancelled:
				if is_individual_download:
					path_text = ""
					if isinstance(saved_items, list) and saved_items:
						first_saved = saved_items[0]
						if isinstance(first_saved, dict):
							path_text = str(first_saved.get("path", "") or "")
					if not path_text and kwargs.get("records"):
						first_record = kwargs.get("records")[0]
						if isinstance(first_record, dict):
							existing_path = find_existing_pdf_path(
								Path(kwargs["output_dir"]) / "pdf",
								first_record,
								normalize_file_name_mode(str(kwargs.get("file_name_mode", "doi") or "doi")),
							)
							if existing_path is not None:
								path_text = str(existing_path)
					if path_text:
						self.log_queue.put(("open_path_prompt", {"kind": "file", "path": path_text}))
				else:
					folder_path = str(Path(kwargs["output_dir"]) / "pdf")
					self.log_queue.put(("open_path_prompt", {"kind": "folder", "path": folder_path}))
		except Exception:
			self.log_queue.put(("debug", traceback.format_exc()))
			self.log_queue.put(("error", "PDF download failed. Check network/API credentials."))
		finally:
			self.log_queue.put(("state", "idle_download"))

	def _poll_log_queue(self) -> None:
		try:
			while True:
				event_type, payload = self.log_queue.get_nowait()
				if event_type == "progress":
					current, total, message = payload
					self._update_progress_window(current, total, message)
				elif event_type == "table_reset":
					self.clear_results()
					total = int(payload or 0)
					self.summary_var.set(f"Streaming results: 0/{total}")
				elif event_type == "record_row":
					index, total, record = payload
					self._append_result_row(record, index)
					self.summary_var.set(f"Streaming results: {index}/{total}")
				elif event_type == "csv_saved":
					self.status_label.configure(text="CSV ready")
				elif event_type == "search_summary":
					self.last_search_summary = payload
					self.cached_search_context = {
						"records": list(payload.get("records", [])),
						"raw_keywords": payload.get("raw_keywords", ""),
						"raw_title": payload.get("raw_title", ""),
						"start_year": payload.get("start_year"),
						"end_year": payload.get("end_year"),
					}
					self._add_search_history_entry(payload)
					self.summary_var.set(
						f"Selected {payload['records_total']} records | "
						f"Unique before rank-limit: {payload.get('records_unique', payload['records_total'])} | "
						f"Elsevier: {payload['records_elsevier']} | "
						f"PubMed: {payload['records_pubmed']}"
					)
				elif event_type == "ask_download":
					self._close_progress_window()
					self._open_download_prompt(payload)
				elif event_type == "download_summary":
					if self._pending_download_backup is not None:
						self._set_pending_download(self._pending_download_backup, visible=True)
						self._pending_download_backup = None
					else:
						self._set_pending_download(None, visible=False)
					if isinstance(payload, dict):
						self._mark_failed_download_rows(payload.get("failed_items", []))
					else:
						self._clear_failed_download_row_marks()
					search = self.last_search_summary or {}
					self.summary_var.set(
						f"Selected {search.get('records_total', 0)} records | "
						f"Unique before rank-limit: {search.get('records_unique', search.get('records_total', 0))} | "
						f"OA-only: {search.get('records_open_access', search.get('records_total', 0))} | "
						f"Elsevier: {search.get('records_elsevier', 0)} | "
						f"PubMed: {search.get('records_pubmed', 0)} | "
						f"Saved: {payload['saved']} | Skipped: {payload['skipped']} | Failed: {payload['failed']}"
					)
				elif event_type == "debug":
					print(payload)
				elif event_type == "ok":
					self._hide_download_failure_block()
					messagebox.showinfo("Success", payload)
				elif event_type == "warn":
					messagebox.showwarning("Download Warning", payload)
				elif event_type == "open_path_prompt":
					if isinstance(payload, dict):
						self._show_open_location_prompt(
							str(payload.get("kind", "folder")),
							str(payload.get("path", "")),
						)
				elif event_type == "error":
					self._close_progress_window()
					self._close_download_prompt()
					if self._pending_download_backup is not None:
						self._set_pending_download(self._pending_download_backup, visible=True)
						self._pending_download_backup = None
					self._set_running(False, "Ready")
					messagebox.showerror("Error", payload)
				elif event_type == "state" and payload == "idle_search":
					self._set_running(False, "Ready")
				elif event_type == "state" and payload == "idle_download":
					self._set_running(False, "Ready")
					self._close_progress_window()
		except queue.Empty:
			pass
		self.root.after(150, self._poll_log_queue)

	def run(self) -> None:
		self.root.mainloop()


def main_cli() -> None:
	print("=== DOI Workflow (Elsevier + PubMed) ===")
	print("Tip: set env vars ELSEVIER_API_KEY, NCBI_EMAIL, NCBI_API_KEY before running.")
	print("Optional for entitlement access: ELSEVIER_BEARER_TOKEN")

	raw_keywords = input("Keywords: ").strip()
	query = build_keyword_query(raw_keywords, "AUTO")
	rank_mode_raw = (input("Top ranker [most_cited/newest] [most_cited]: ").strip() or "most_cited").lower()
	ranking_mode = "newest" if rank_mode_raw == "newest" else "most_cited"

	start_year = to_optional_int(input("Start year (blank = no limit): "))
	end_year = to_optional_int(input("End year (blank = no limit): "))
	max_elsevier = int(input("Max Elsevier records [200]: ").strip() or "200")
	max_pubmed = int(input("Max PubMed records [200]: ").strip() or "200")
	final_top_n = to_optional_int(input("Top ranked records (blank = all): "))
	output_dir_raw = input("Output directory [output/]: ").strip() or "output/"
	output_dir = Path(output_dir_raw)

	summary = run_workflow(
		query=query,
		start_year=start_year,
		end_year=end_year,
		max_elsevier=max_elsevier,
		max_pubmed=max_pubmed,
		final_top_n=final_top_n,
		output_dir=output_dir,
		skip_download=True,
		save_csv=True,
		ranking_mode=ranking_mode,
	)

	print(f"[OK] Table-ready records: {summary.get('records_total', 0)}")
	download_now = to_bool(input("Download PDFs now? (y/n) [n]: ").strip() or "n")
	if download_now:
		run_pdf_download(records=summary.get("records", []), output_dir=output_dir)


def main() -> None:
	app = RetrieveGUI()
	app.run()


if __name__ == "__main__":
	if "--cli" in sys.argv:
		main_cli()
	else:
		main()

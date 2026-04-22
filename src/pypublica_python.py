import csv
import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import quote

import requests


SCOPUS_SEARCH_URL = "https://api.elsevier.com/content/search/scopus"

PUBMED_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
PUBMED_EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"


def normalize_doi(doi: str) -> str:
	return doi.strip().lower()


def ensure_dir(path: Path) -> None:
	path.mkdir(parents=True, exist_ok=True)


def safe_filename(value: str) -> str:
	cleaned = []
	for char in value:
		if char.isalnum() or char in {"-", "_", "."}:
			cleaned.append(char)
		else:
			cleaned.append("_")
	return "".join(cleaned)


def request_with_retry(
	session: requests.Session,
	method: str,
	url: str,
	*,
	retries: int = 3,
	sleep_s: float = 1.0,
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
			time.sleep(sleep_s * attempt)
	raise RuntimeError(f"Request failed after {retries} attempts: {url}\n{last_error}")


def build_scopus_query(
	q_base: str,
	s_year: Optional[int],
	e_year: Optional[int],
	oa_only: bool = True,
) -> str:
	query = q_base.strip()
	if oa_only:
		query += " AND OPENACCESS(1)"
	if s_year and e_year:
		query += f" AND PUBYEAR > {s_year - 1} AND PUBYEAR < {e_year + 1}"
	elif s_year:
		query += f" AND PUBYEAR > {s_year - 1}"
	elif e_year:
		query += f" AND PUBYEAR < {e_year + 1}"
	return query


def search_elsevier_scopus_dois(
	session: requests.Session,
	a_key: str,
	query: str,
	s_year: Optional[int],
	e_year: Optional[int],
	max_res: int,
	b_tok: Optional[str] = None,
	cnt_page: int = 25,
) -> List[Dict[str, str]]:
	recs_all: List[Dict[str, str]] = []
	start = 0
	q_final = build_scopus_query(query, s_year, e_year, oa_only=True)

	headers = {
		"X-ELS-APIKey": a_key,
		"Accept": "application/json",
	}
	if b_tok:
		headers["Authorization"] = f"Bearer {b_tok}"

	while len(recs_all) < max_res:
		params = {
			"query": q_final,
			"start": start,
			"count": min(cnt_page, max_res - len(recs_all)),
			"apiKey": a_key,
			"field": "dc:identifier,dc:title,prism:doi,prism:coverDate,prism:publicationName,openaccess,freetoread",
		}
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

			recs_all.append(
				{
					"doi": normalize_doi(doi),
					"source": "elsevier",
					"open_access": open_access_value,
					"title": item.get("dc:title", ""),
					"date": item.get("prism:coverDate", ""),
					"journal": item.get("prism:publicationName", ""),
					"id": item.get("dc:identifier", ""),
				}
			)

		start += len(entries)
		if len(entries) < params["count"]:
			break

	return recs_all


def build_pubmed_term(q_base: str, s_year: Optional[int], e_year: Optional[int]) -> str:
	q_parts = [f"({q_base.strip()})", "(hasabstract[text])"]
	if s_year and e_year:
		q_parts.append(f'("{s_year}"[Date - Publication] : "{e_year}"[Date - Publication])')
	elif s_year:
		q_parts.append(f'("{s_year}"[Date - Publication] : "3000"[Date - Publication])')
	elif e_year:
		q_parts.append(f'("1800"[Date - Publication] : "{e_year}"[Date - Publication])')
	return " AND ".join(q_parts)


def esearch_pubmed_ids(
	session: requests.Session,
	term: str,
	max_res: int,
	mail: Optional[str],
	a_key: Optional[str],
) -> List[str]:
	retstart = 0
	retmax = 200
	ids: List[str] = []

	while len(ids) < max_res:
		params = {
			"db": "pubmed",
			"term": term,
			"retmode": "json",
			"retmax": min(retmax, max_res - len(ids)),
			"retstart": retstart,
		}
		if mail:
			params["email"] = mail
		if a_key:
			params["api_key"] = a_key

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
	mail: Optional[str],
	a_key: Optional[str],
) -> str:
	params = {
		"db": "pubmed",
		"id": ",".join(pmids),
		"retmode": "xml",
	}
	if mail:
		params["email"] = mail
	if a_key:
		params["api_key"] = a_key

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
		title_key = title.lower()
		if title_key in seen_titles:
			continue

		journal_node = article.find(".//Article/Journal/Title")
		journal = journal_node.text.strip() if journal_node is not None and journal_node.text else ""

		year_node = article.find(".//Article/Journal/JournalIssue/PubDate/Year")
		year = year_node.text.strip() if year_node is not None and year_node.text else ""
		doi_node = article.find('.//ArticleIdList/ArticleId[@IdType="doi"]')
		if doi_node is not None and doi_node.text:
			seen_titles.add(title_key)
			records.append(
				{
					"doi": normalize_doi(doi_node.text),
					"source": "pubmed",
					"open_access": "",
					"title": title,
					"date": year,
					"journal": journal,
					"id": pmid,
				}
			)

	return records


def search_pubmed_dois(
	session: requests.Session,
	query: str,
	s_year: Optional[int],
	e_year: Optional[int],
	max_res: int,
	mail: Optional[str],
	a_key: Optional[str],
) -> List[Dict[str, str]]:
	term = build_pubmed_term(query, s_year, e_year)
	pmids = esearch_pubmed_ids(session, term, max_res=max_res, mail=mail, a_key=a_key)
	if not pmids:
		return []

	recs_all: List[Dict[str, str]] = []
	chunk_size = 100
	for i in range(0, len(pmids), chunk_size):
		chunk = pmids[i : i + chunk_size]
		x_txt = fetch_pubmed_records_xml(session, chunk, mail=mail, a_key=a_key)
		parsed = parse_pubmed_doi_map(x_txt)
		recs_all.extend(parsed)
		time.sleep(0.34 if a_key else 0.5)

	return recs_all[:max_res]


def deduplicate_records(records: List[Dict[str, str]]) -> List[Dict[str, str]]:
	seen: Set[Tuple[str, str]] = set()
	unique: List[Dict[str, str]] = []
	for rec in records:
		key = (rec["doi"], rec["source"])
		if key in seen:
			continue
		seen.add(key)
		unique.append(rec)
	return unique


def save_doi_csv(records: List[Dict[str, str]], out_csv: Path) -> None:
	ensure_dir(out_csv.parent)
	with out_csv.open("w", newline="", encoding="utf-8") as file:
		writer = csv.DictWriter(
			file,
			fieldnames=["doi", "source", "open_access", "title", "date", "journal", "id"],
		)
		writer.writeheader()
		writer.writerows(records)


def download_via_unpaywall(
	session: requests.Session,
	doi: str,
	mail: str,
) -> Optional[bytes]:
	api_url = f"https://api.unpaywall.org/v2/{quote(doi, safe='/')}"
	try:
		response = request_with_retry(
			session, "GET", api_url, params={"email": mail}
		)
		data = response.json()
		pdf_url = None
		boa = data.get("best_oa_location") or {}
		pdf_url = boa.get("url_for_pdf") or boa.get("url")
		if not pdf_url:
			for loc in data.get("oa_locations", []):
				candidate = loc.get("url_for_pdf") or loc.get("url")
				if candidate:
					pdf_url = candidate
					break
		if not pdf_url:
			return None
		pdf_resp = request_with_retry(session, "GET", pdf_url)
		if pdf_resp.content[:5] == b"%PDF-" or "pdf" in pdf_resp.headers.get("Content-Type", ""):
			return pdf_resp.content
		return None
	except (RuntimeError, Exception) as error:
		print(f"[WARN] Unpaywall download failed for DOI {doi}: {error}")
		return None


def download_unpaywall_pdfs_for_records(
	session: requests.Session,
	records: List[Dict[str, str]],
	out_dir: Path,
	unpaywall_email: str,
) -> Dict[str, int]:
	ensure_dir(out_dir)
	stats = {"saved": 0, "skipped": 0, "failed": 0}

	for record in records:
		doi = record["doi"]
		source = record["source"]
		src_dir = out_dir / source
		ensure_dir(src_dir)

		b_name = safe_filename(doi)
		p_path = src_dir / f"{b_name}.pdf"

		if p_path.exists():
			stats["skipped"] += 1
			continue

		p_data = download_via_unpaywall(session, doi, unpaywall_email)
		if p_data:
			p_path.write_bytes(p_data)
			stats["saved"] += 1
		else:
			print(f"[FAIL] Could not download DOI {doi} via Unpaywall")
			stats["failed"] += 1

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
	if "(" in trimmed and ")" in trimmed:
		return trimmed
	return f"TITLE-ABS-KEY({trimmed})"


def run_workflow(
	query: str,
	s_year: Optional[int],
	e_year: Optional[int],
	max_els: int,
	max_pub: int,
	out_dir: Path,
	skip_dl: bool,
) -> None:
	els_key = os.getenv("ELSEVIER_API_KEY")
	els_tok = os.getenv("ELSEVIER_BEARER_TOKEN")
	n_mail = os.getenv("NCBI_EMAIL")
	n_key = os.getenv("NCBI_API_KEY")
	up_mail = os.getenv("UNPAYWALL_EMAIL") or n_mail or ""

	with requests.Session() as session:
		recs_els: List[Dict[str, str]] = []
		if els_key:
			elsevier_query = normalize_scopus_input(query)
			recs_els = search_elsevier_scopus_dois(
				session,
				a_key=els_key,
				query=elsevier_query,
				s_year=s_year,
				e_year=e_year,
				max_res=max_els,
				b_tok=els_tok,
			)
		else:
			print("[WARN] ELSEVIER_API_KEY not set. Skipping Elsevier fetch.")

		recs_pub = search_pubmed_dois(
			session,
			query=query,
			s_year=s_year,
			e_year=e_year,
			max_res=max_pub,
			mail=n_mail,
			a_key=n_key,
		)

		recs_all = deduplicate_records(recs_els + recs_pub)

		csv_path = out_dir / "doi_list.csv"
		save_doi_csv(recs_all, csv_path)
		print(f"[OK] DOI list saved: {csv_path} ({len(recs_all)} records)")
		print(f"      Elsevier: {len(recs_els)} | PubMed: {len(recs_pub)}")

		if not skip_dl:
			if not up_mail:
				print("[WARN] UNPAYWALL_EMAIL (or NCBI_EMAIL) is not set. Skipping PDF download.")
			else:
				stats = download_unpaywall_pdfs_for_records(
					session,
					recs_all,
					out_dir=out_dir / "pdf",
					unpaywall_email=up_mail,
				)
				print(
					"[OK] Unpaywall PDF download finished | "
					f"saved={stats['saved']} skipped={stats['skipped']} failed={stats['failed']}"
				)


def main() -> None:
	print("=== DOI Workflow (Elsevier + PubMed) ===")

	query = input("Search query (example: cancer immunotherapy): ").strip()
	if not query:
		raise ValueError("Query is required.")

	s_year = to_optional_int(input("Start year (blank = no limit): "))
	e_year = to_optional_int(input("End year (blank = no limit): "))
	max_els = int(input("Max Elsevier records [200]: ").strip() or "200")
	max_pub = int(input("Max PubMed records [200]: ").strip() or "200")
	out_raw = input("Output directory [output/doi_workflow]: ").strip() or "output/doi_workflow"
	skip_dl = to_bool(input("Skip PDF download? (y/n) [n]: ").strip() or "n")

	run_workflow(
		query=query,
		s_year=s_year,
		e_year=e_year,
		max_els=max_els,
		max_pub=max_pub,
		out_dir=Path(out_raw),
		skip_dl=skip_dl,
	)


if __name__ == "__main__":
	main()


import os
import requests
import time

def download_pdfs():
    os.makedirs("data/raw/pdfs", exist_ok=True)

    headers = {
        'User-Agent': 'Adityaram Komaraneni ak5480@columbia.edu'
    }

    search_url = "https://efts.sec.gov/LATEST/search-index?q=%22expense+ratio%22+%22net+assets%22&forms=497K&dateRange=custom&startdt=2024-01-01&enddt=2024-06-01"

    print("Fetching ETF filings from SEC EDGAR...")
    response = requests.get(search_url, headers=headers)
    data = response.json()
    hits = data.get('hits', {}).get('hits', [])
    print(f"Found {len(hits)} filings\n")

    downloaded = 0

    for hit in hits:
        try:
            source = hit.get('_source', {})
            adsh = source.get('adsh', '')
            ciks = source.get('ciks', [])
            name = source.get('display_names', ['unknown'])[0]

            if not adsh or not ciks:
                continue

            cik = ciks[0].lstrip('0')
            adsh_clean = adsh.replace('-', '')

            # Construct index page URL
            index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{adsh_clean}/{adsh}-index.htm"

            # Fetch the index page to find the actual document
            index_resp = requests.get(index_url, headers=headers, timeout=10)
            if index_resp.status_code != 200:
                continue

            # Find the primary document link in the index
            from html.parser import HTMLParser

            class LinkParser(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.links = []

                def handle_starttag(self, tag, attrs):
                    if tag == 'a':
                        for attr, val in attrs:
                            if attr == 'href' and val and (
                                val.endswith('.htm') or
                                val.endswith('.html') or
                                val.endswith('.txt')
                            ) and 'index' not in val:
                                self.links.append(val)

            parser = LinkParser()
            parser.feed(index_resp.text)

            if not parser.links:
                continue

            # Get the first document link
            doc_path = parser.links[0]
            if not doc_path.startswith('http'):
                doc_url = f"https://www.sec.gov{doc_path}"
            else:
                doc_url = doc_path

            doc_resp = requests.get(doc_url, headers=headers, timeout=10)
            if doc_resp.status_code != 200:
                continue

            # Save it
            safe_name = name[:40].replace(' ', '_').replace('/', '_').replace(',', '')
            filename = f"data/raw/pdfs/{safe_name}_{downloaded}.htm"
            with open(filename, 'wb') as f:
                f.write(doc_resp.content)

            print(f"  ✅ [{downloaded+1}] {name[:50]}")
            downloaded += 1
            time.sleep(0.3)

            if downloaded >= 15:
                break

        except Exception as e:
            continue

    print(f"\nDownloaded {downloaded} filings to data/raw/pdfs/")

if __name__ == "__main__":
    download_pdfs()
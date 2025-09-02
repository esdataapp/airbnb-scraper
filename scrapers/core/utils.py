import re

def extract_listing_id(url):
    if not url: return None
    m=re.search(r"/rooms/(\d+)",url)
    return m.group(1) if m else None

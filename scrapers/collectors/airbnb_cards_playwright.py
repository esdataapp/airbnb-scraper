import re,json
from typing import List,Dict,Optional
from playwright.async_api import async_playwright
from ..core.metrics import log
from ..core.utils import extract_listing_id

def _as_int(t: Optional[str]):
    if not t: return None
    m=re.findall(r"\d+", t.replace(',',''))
    return int(''.join(m)) if m else None

async def collect_from_search(page, url: str, max_scrolls: int=8, max_listings: int=500,
                              snapshot_path: Optional[str]=None, snapshot_ctx: Optional[Dict]=None) -> List[Dict]:
    await page.goto(url, wait_until='domcontentloaded', timeout=60000)
    await page.wait_for_timeout(1200)
    data=[]; seen=set()
    async def first_text(h, sels):
        for s in sels:
            el=await h.query_selector(s)
            if el:
                tx=(await el.inner_text()).strip()
                if tx: return tx
        return None
    async def scrape_once():
        anchors=await page.query_selector_all('a[href*="/rooms/"]:not([aria-hidden="true"])')
        for a in anchors:
            href=await a.get_attribute('href')
            if not href: continue
            if href.startswith('/'):
                href='https://www.airbnb.mx'+href
            lid=extract_listing_id(href)
            if not lid or lid in seen: continue
            card=await a.evaluate_handle("el => el.closest('div[data-testid], li, article, div')")
            title=await first_text(card,['[data-testid="listing-card-title"]','div[style*="-webkit-line-clamp"]','span[aria-hidden="true"]'])
            price_n=await first_text(card,['span[data-testid="price"]','span:has-text("$")'])
            price_t=await first_text(card,['span[data-testid="price-detail-total"]','span:has-text("total")'])
            rating=None; reviews=None
            star=await card.query_selector('span[aria-label*="estrellas"], span:has-text("★")')
            if star:
                st=(await star.inner_text()).strip(); m=re.search(r'([0-9](?:\.[0-9])?)',st); rating=m.group(1) if m else None
            rev=await card.query_selector('span[aria-label*="reseña"], span:has-text("reseña")')
            if rev:
                rt=(await rev.inner_text()).strip(); m=re.search(r'(\d[\d,\.]*)',rt); reviews=m.group(1).replace(',','') if m else None
            subt=await first_text(card,['[data-testid="listing-card-subtitle"]'])
            tipo=subt.split(' · ')[0].strip() if subt and ' · ' in subt else (subt or None)
            ubic=subt
            superhost=True if await card.query_selector('span:has-text("Superanfitrión")') else None
            if snapshot_path:
                try:
                    html=await card.inner_html(); snap={'listing_id':lid,'html':html}
                    if snapshot_ctx: snap.update(snapshot_ctx)
                    with open(snapshot_path,'a',encoding='utf-8') as sf:
                        sf.write(json.dumps(snap, ensure_ascii=False)+'\n')
                except Exception as e:
                    log('snapshot_err', listing_id=lid, error=str(e))
            data.append({'listing_id':lid,'titulo':title,'precio_noche':price_n,'precio_noche_mxn':_as_int(price_n),
                         'precio_total_aprox':price_t,'precio_total_aprox_mxn':_as_int(price_t),
                         'rating':rating,'reviews':reviews,'tipo_lugar':tipo,'superhost':superhost,'ubicacion':ubic,'url':href})
            seen.add(lid)
    await scrape_once()
    for _ in range(max_scrolls):
        await page.mouse.wheel(0,1800); await page.wait_for_timeout(700); await scrape_once()
        if len(seen)>=max_listings: break
    return data[:max_listings]

async def run_single(url: str, headless: bool=True, max_scrolls: int=8, max_listings: int=500,
                     snapshot_path: Optional[str]=None, snapshot_ctx: Optional[Dict]=None) -> List[Dict]:
    async with async_playwright() as p:
        b=await p.chromium.launch(headless=headless)
        ctx=await b.new_context(locale='es-MX'); page=await ctx.new_page(); log('open_search', url=url)
        rows=await collect_from_search(page, url, max_scrolls=max_scrolls, max_listings=max_listings,
                                       snapshot_path=snapshot_path, snapshot_ctx=snapshot_ctx)
        await b.close(); log('search_done', url=url, n=len(rows)); return rows

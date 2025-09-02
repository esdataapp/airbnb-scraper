import csv,json,time,urllib.parse,argparse
from pathlib import Path
from ..collectors.airbnb_cards_playwright import run_single
from ..core.metrics import log

def build_url(city_slug, checkin, checkout, adults, colonia):
    base=f'https://www.airbnb.mx/s/{city_slug}/homes'
    q=urllib.parse.quote_plus(f'{colonia}, Jalisco')
    return (f"{base}?refinement_paths%5B%5D=%2Fhomes&search_mode=regular_search&checkin={checkin}&checkout={checkout}&adults={adults}&query={q}")

def _read_panpoints(p, city_filter):
    rows=[]
    with open(p, newline='', encoding='utf-8') as f:
        rdr=csv.DictReader(f)
        for r in rdr:
            if city_filter and r.get('city') not in city_filter: continue
            rows.append(r)
    return rows

async def sweep(panpoints_csv: Path, city_filter, checkin, checkout, adults, headless, max_scrolls, max_listings,
                out_csv: Path, coverage_jsonl: Path, order='density', snapshots_path: Path=Path('data/snapshots/cards.ndjson'),
                summary_csv: Path=Path('data/out/summary_by_colonia.csv')):
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    coverage_jsonl.parent.mkdir(parents=True, exist_ok=True)
    snapshots_path.parent.mkdir(parents=True, exist_ok=True)
    summary_csv.parent.mkdir(parents=True, exist_ok=True)
    seen=set(); write_header=not out_csv.exists()
    pp=_read_panpoints(panpoints_csv, city_filter)
    dens={}
    for r in pp:
        c=r['colonia']; dens[c]=dens.get(c,0)+int(r.get('n_samples',1) or 1)
    cols=list(dens.keys())
    if order=='density': cols.sort(key=lambda c: dens.get(c,0), reverse=True)
    agg={c:{'city':None,'colonia':c,'n_samples':0,'new_ids':0,'elapsed_s':0.0,'price_mxn':[],'rating':[],'reviews':[]} for c in cols}
    col_city={}
    for r in pp:
        c=r['colonia']
        if c not in col_city: col_city[c]=r.get('city')
        agg[c]['n_samples']=max(agg[c]['n_samples'], int(r.get('n_samples',1) or 1))
        agg[c]['city']=col_city[c]
    with open(out_csv,'a',newline='',encoding='utf-8') as fo:
        w=csv.DictWriter(fo, fieldnames=['listing_id','titulo','precio_noche','precio_noche_mxn','precio_total_aprox','precio_total_aprox_mxn','rating','reviews','tipo_lugar','superhost','ubicacion','url','city','colonia'])
        if write_header: w.writeheader()
        for c in cols:
            city=col_city[c]; slug='Guadalajara' if city=='gdl' else 'Zapopan'
            url=build_url(slug, checkin, checkout, adults, c)
            t0=time.perf_counter()
            try:
                rows=await run_single(url, headless=headless, max_scrolls=max_scrolls, max_listings=max_listings,
                                      snapshot_path=str(snapshots_path), snapshot_ctx={'city':city,'colonia':c})
            except Exception as e:
                log('sweep_error', colonia=c, error=str(e)); rows=[]
            new=0
            for r in rows:
                lid=r.get('listing_id')
                if lid and lid not in seen:
                    seen.add(lid); new+=1
                    r['city']=city; r['colonia']=c; w.writerow(r)
                    if r.get('precio_noche_mxn'): agg[c]['price_mxn'].append(int(r['precio_noche_mxn']))
                    if r.get('rating'):
                        try: agg[c]['rating'].append(float(r['rating']))
                        except: pass
                    if r.get('reviews'):
                        try: agg[c]['reviews'].append(int(str(r['reviews']).replace(',','')))
                        except: pass
            el=round(time.perf_counter()-t0,2)
            agg[c]['new_ids']+=new; agg[c]['elapsed_s']+=el
            with open(coverage_jsonl,'a',encoding='utf-8') as cov:
                cov.write(json.dumps({'city':city,'colonia':c,'new_ids':new,'total_ids':len(seen),'elapsed_s':el}, ensure_ascii=False)+'\n')
            log('colonia_done', colonia=c, new_ids=new, total_ids=len(seen), elapsed_s=el)
    tot=max(1,len(seen))
    with open(summary_csv,'w',newline='',encoding='utf-8') as sf:
        w=csv.DictWriter(sf, fieldnames=['city','colonia','n_samples','new_ids','pct_total_ids','elapsed_s','price_avg_mxn','price_median_mxn','price_min_mxn','price_max_mxn','rating_avg','reviews_median'])
        w.writeheader()
        for c,rec in agg.items():
            P=rec['price_mxn']; R=rec['rating']; V=rec['reviews']
            w.writerow({'city':rec['city'],'colonia':c,'n_samples':rec['n_samples'],'new_ids':rec['new_ids'],'pct_total_ids':round(100.0*rec['new_ids']/tot,2),'elapsed_s':round(rec['elapsed_s'],2),'price_avg_mxn':round(sum(P)/len(P),2) if P else None,'price_median_mxn':(sorted(P)[len(P)//2] if P else None),'price_min_mxn':(min(P) if P else None),'price_max_mxn':(max(P) if P else None),'rating_avg':round(sum(R)/len(R),2) if R else None,'reviews_median':(sorted(V)[len(V)//2] if V else None)})
    log('sweep_done', total_ids=len(seen), summary=str(summary_csv))

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--panpoints', default='data/panpoints/airbnb_panpoints_gdl_zap.csv')
    ap.add_argument('--cities', default='gdl,zap')
    ap.add_argument('--checkin', default='2025-09-12'); ap.add_argument('--checkout', default='2025-09-16')
    ap.add_argument('--adults', type=int, default=2); ap.add_argument('--headless', action='store_true', default=True)
    ap.add_argument('--max-scrolls', type=int, default=8); ap.add_argument('--max-listings', type=int, default=500)
    ap.add_argument('--out-csv', default='data/out/results.csv'); ap.add_argument('--coverage', default='data/coverage/coverage.jsonl')
    ap.add_argument('--order', choices=['density','as-is'], default='density')
    ap.add_argument('--snapshots', default='data/snapshots/cards.ndjson'); ap.add_argument('--summary', default='data/out/summary_by_colonia.csv')
    a=ap.parse_args(); import asyncio
    asyncio.run(sweep(Path(a.panpoints), set(a.cities.split(',')), a.checkin, a.checkout, a.adults, a.headless, a.max_scrolls, a.max_listings, Path(a.out_csv), Path(a.coverage), a.order, Path(a.snapshots), Path(a.summary)))

if __name__=='__main__': main()

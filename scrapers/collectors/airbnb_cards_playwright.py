import re, json, asyncio, random
from typing import List, Dict, Optional
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from ..core.metrics import log
from ..core.utils import extract_listing_id

def _as_int(t: Optional[str]):
    if not t: return None
    m = re.findall(r"\d+", t.replace(',',''))
    return int(''.join(m)) if m else None

def _extract_price_mxn(price_text: str) -> Optional[int]:
    """Extraer precio numérico en MXN de texto de precio"""
    if not price_text: return None
    # Buscar patrones como "$1,234 MXN" o "$1,234"
    m = re.search(r'\$?([\d,]+)', price_text.replace(' ', ''))
    if m:
        return int(m.group(1).replace(',', ''))
    return None

async def professional_scroll_and_load(page, max_scrolls: int = 15, patience: int = 3):
    """Scroll profesional con detección de nueva contenido"""
    log("scroll_start", max_scrolls=max_scrolls)
    
    previous_count = 0
    stagnant_rounds = 0
    
    for scroll_round in range(max_scrolls):
        # Scroll más agresivo y realista
        await page.evaluate("""
            () => {
                const scrollHeight = document.body.scrollHeight;
                const currentScroll = window.pageYOffset;
                const clientHeight = window.innerHeight;
                const scrollAmount = Math.min(clientHeight * 0.8, scrollHeight - currentScroll);
                window.scrollBy(0, scrollAmount);
            }
        """)
        
        # Pausa humanizada
        await asyncio.sleep(random.uniform(1.2, 2.5))
        
        # Contar elementos actuales
        current_count = len(await page.query_selector_all('a[href*="/rooms/"]'))
        
        if current_count > previous_count:
            log("scroll_progress", round=scroll_round + 1, count=current_count, new=current_count - previous_count)
            stagnant_rounds = 0
        else:
            stagnant_rounds += 1
            log("scroll_stagnant", round=scroll_round + 1, count=current_count)
        
        previous_count = current_count
        
        # Detener si no hay nuevos elementos por varias rondas
        if stagnant_rounds >= patience:
            log("scroll_stop", reason="no_new_elements", patience=patience)
            break
    
    log("scroll_complete", total_listings=previous_count)

async def extract_card_data(card, listing_id: str, href: str) -> Dict:
    """Extraer datos detallados de una tarjeta de propiedad"""
    property_data = {
        'listing_id': listing_id,
        'url': href,
        'titulo': None,
        'precio_noche': None,
        'precio_noche_mxn': None,
        'precio_total_aprox': None,
        'rating': None,
        'reviews': None,
        'tipo_lugar': None,
        'superhost': None,
        'ubicacion': None
    }
    
    # Título con múltiples selectores robustos
    title_selectors = [
        '[data-testid="listing-card-title"]',
        'div[style*="-webkit-line-clamp"]',
        'span[aria-hidden="true"]',
        'h3',
        'h2',
        '.listing-title',
        'div[data-testid="listing-card-title"] span',
        'span[data-testid="listing-card-name"]'
    ]
    
    for sel in title_selectors:
        try:
            el = await card.query_selector(sel)
            if el:
                text = (await el.inner_text()).strip()
                if text and len(text) > 3:  # Filtrar títulos muy cortos
                    property_data['titulo'] = text
                    break
        except:
            continue
    
    # Precio con análisis mejorado - múltiples estrategias
    price_selectors = [
        'span[data-testid="price"]',
        'div[data-testid="price-availability-message"]',
        'span[data-testid="price-value"]',
        'span:has-text("$")',
        '.price',
        'div:has-text("$") span',
        '[data-testid="price-section"] span'
    ]
    
    for sel in price_selectors:
        try:
            el = await card.query_selector(sel)
            if el:
                price_text = (await el.inner_text()).strip()
                if '$' in price_text and len(price_text) > 2:
                    property_data['precio_noche'] = price_text
                    property_data['precio_noche_mxn'] = _extract_price_mxn(price_text)
                    break
        except:
            continue
    
    # Rating mejorado con múltiples estrategias
    rating_selectors = [
        'span[aria-label*="estrellas"]',
        'span[aria-label*="rating"]',
        'span:has-text("★")',
        'div[role="img"][aria-label*="estrellas"]',
        'span[data-testid="rating"]',
        'div[aria-label*="calificación"]'
    ]
    
    for sel in rating_selectors:
        try:
            el = await card.query_selector(sel)
            if el:
                rating_text = (await el.inner_text()).strip()
                # Buscar patrones como "4.8", "4,8", "★4.8"
                m = re.search(r'([0-5](?:[.,]\d)?)', rating_text.replace(',', '.'))
                if m:
                    rating_val = float(m.group(1))
                    if 0 <= rating_val <= 5:
                        property_data['rating'] = rating_val
                        break
        except:
            continue
    
    # Reviews con mejor detección
    review_selectors = [
        'span[aria-label*="reseña"]',
        'span[aria-label*="comentario"]',
        'span:has-text("reseña")',
        'span:has-text("comentario")',
        'span[data-testid="reviews-count"]',
        'button:has-text("reseña")',
        'span[aria-label*="reseñas"]'
    ]
    
    for sel in review_selectors:
        try:
            el = await card.query_selector(sel)
            if el:
                review_text = (await el.inner_text()).strip()
                # Buscar números en el texto de reviews
                m = re.search(r'(\d{1,4})', review_text.replace(',', '').replace('.', ''))
                if m:
                    reviews_count = int(m.group(1))
                    if reviews_count > 0:
                        property_data['reviews'] = reviews_count
                        break
        except:
            continue
    
    # Superhost con múltiples indicadores
    try:
        superhost_selectors = [
            'span:has-text("Superanfitrión")',
            'span:has-text("Superhost")',
            'div:has-text("Superanfitrión")',
            '[data-testid="superhost-badge"]',
            '.superhost-badge'
        ]
        
        for sel in superhost_selectors:
            superhost_el = await card.query_selector(sel)
            if superhost_el:
                property_data['superhost'] = True
                break
        
        if property_data['superhost'] is None:
            property_data['superhost'] = False
    except:
        property_data['superhost'] = False
    
    # Subtítulo para tipo y ubicación
    subtitle_selectors = [
        '[data-testid="listing-card-subtitle"]',
        '.listing-subtitle',
        'div[data-testid="listing-card-subtitle"] span',
        'span[data-testid="listing-card-subtitle"]'
    ]
    
    for sel in subtitle_selectors:
        try:
            el = await card.query_selector(sel)
            if el:
                subtitle = (await el.inner_text()).strip()
                if subtitle and len(subtitle) > 3:
                    # Separar tipo y ubicación si contiene ·
                    if ' · ' in subtitle:
                        parts = subtitle.split(' · ')
                        if len(parts) >= 2:
                            property_data['tipo_lugar'] = parts[0].strip()
                            property_data['ubicacion'] = ' · '.join(parts[1:]).strip()
                    else:
                        property_data['ubicacion'] = subtitle
                    break
        except:
            continue
    
    return property_data

async def collect_from_search(page, url: str, max_scrolls: int = 15, max_listings: int = 500,
                              snapshot_path: Optional[str] = None, snapshot_ctx: Optional[Dict] = None) -> List[Dict]:
    """Collector profesional mejorado basado en técnicas de scrapers avanzados"""
    
    log("extraction_start", url=url)
    
    try:
        # Carga inicial con retry estratégico
        for attempt in range(3):
            try:
                await page.goto(url, wait_until='networkidle', timeout=60000)
                break
            except PWTimeout:
                log("timeout_warning", attempt=attempt + 1)
                if attempt == 2:
                    await page.goto(url, wait_until='domcontentloaded', timeout=60000)
        
        # Espera inicial y manejo de cookies
        await asyncio.sleep(2)
        
        # Aceptar cookies si aparecen - múltiples variantes
        try:
            cookie_selectors = [
                'button:has-text("Aceptar")',
                'button:has-text("Acepto")', 
                'button:has-text("Accept")',
                'button:has-text("OK")',
                'button[data-testid="accept-btn"]',
                '.cookie-banner button'
            ]
            
            for sel in cookie_selectors:
                buttons = await page.query_selector_all(sel)
                if buttons:
                    await buttons[0].click()
                    await asyncio.sleep(1)
                    break
        except:
            pass
        
        # Scroll profesional para cargar contenido dinámico
        await professional_scroll_and_load(page, max_scrolls)
        
        # Extracción de datos con múltiples estrategias
        data = []
        seen = set()
        
        # Selectores expandidos para máxima cobertura
        listing_selectors = [
            'a[href*="/rooms/"]:not([aria-hidden="true"])',
            'a[href*="/rooms/"]',
            '[data-testid="listing-card"] a',
            '[data-testid="property-card"] a',
            '.listing-card a',
            'div[data-listing-id] a',
            'article a[href*="/rooms/"]',
            'li a[href*="/rooms/"]'
        ]
        
        anchors = []
        for selector in listing_selectors:
            try:
                found = await page.query_selector_all(selector)
                anchors.extend(found)
                if found:
                    log("selector_success", selector=selector, count=len(found))
            except Exception as e:
                log("selector_error", selector=selector, error=str(e))
        
        # Eliminar duplicados basado en href
        unique_anchors = []
        href_seen = set()
        for anchor in anchors:
            try:
                href = await anchor.get_attribute('href')
                if href and href not in href_seen:
                    unique_anchors.append(anchor)
                    href_seen.add(href)
            except:
                continue
        
        log("processing_start", unique_links=len(unique_anchors))
        
        # Procesar cada enlace con extracción robusta
        for i, anchor in enumerate(unique_anchors):
            if len(data) >= max_listings:
                break
                
            try:
                href = await anchor.get_attribute('href')
                if not href:
                    continue
                
                # Normalizar URL
                if href.startswith('/'):
                    href = 'https://www.airbnb.mx' + href
                
                # Extraer ID único
                listing_id = extract_listing_id(href)
                if not listing_id or listing_id in seen:
                    continue
                
                seen.add(listing_id)
                
                # Buscar contenedor de tarjeta con estrategias múltiples
                card_containers = [
                    "div[data-testid='listing-card']",
                    "div[data-testid='property-card']",
                    "article",
                    "li",
                    ".listing-card",
                    "div"
                ]
                
                card = None
                for container_sel in card_containers:
                    try:
                        card = await anchor.evaluate_handle(f"el => el.closest('{container_sel}')")
                        if card:
                            break
                    except:
                        continue
                
                if not card:
                    # Fallback: usar el anchor directamente
                    card = anchor
                
                # Extraer datos detallados usando función especializada
                property_data = await extract_card_data(card, listing_id, href)
                
                # Guardar snapshot si se requiere
                if snapshot_path and property_data['titulo']:
                    try:
                        html = await card.inner_html()
                        snap = {
                            'listing_id': listing_id,
                            'html': html,
                            'extracted_data': property_data
                        }
                        if snapshot_ctx:
                            snap.update(snapshot_ctx)
                        
                        with open(snapshot_path, 'a', encoding='utf-8') as sf:
                            sf.write(json.dumps(snap, ensure_ascii=False) + '\n')
                    except Exception as e:
                        log("snapshot_error", listing_id=listing_id, error=str(e))
                
                # Solo agregar si tiene datos mínimos útiles
                if property_data['titulo'] or property_data['precio_noche']:
                    data.append(property_data)
                    
                    if i % 20 == 0:
                        log("progress_update", processed=i + 1, total=len(unique_anchors), extracted=len(data))
                
            except Exception as e:
                log("listing_error", index=i, error=str(e))
                continue
        
        log("extraction_complete", total_extracted=len(data), total_links=len(unique_anchors))
        return data
        
    except Exception as e:
        log("extraction_error", error=str(e))
        return []

async def first_text(card, selectors):
    """Helper mejorado para extraer primer texto encontrado"""
    for selector in selectors:
        try:
            el = await card.query_selector(selector)
            if el:
                text = (await el.inner_text()).strip()
                if text:
                    return text
        except:
            continue
    return None

# Función de compatibilidad con versiones anteriores
async def run_single(url: str, headless: bool = True, max_scrolls: int = 15, max_listings: int = 500,
                      snapshot_path: Optional[str] = None, snapshot_ctx: Optional[Dict] = None) -> List[Dict]:
    """Función compatible para ejecutar scraping individual"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        try:
            results = await collect_from_search(page, url, max_scrolls, max_listings, snapshot_path, snapshot_ctx)
            return results
        finally:
            await browser.close()
